#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
from telebot import TeleBot
from telebot import types
from telebot.apihelper import ApiException
from abc import ABC, abstractmethod

from dialog_machine.project_exceptions import SenderError
from dialog_machine.dialog_machine_core import StateMachine, DialogMachineCore, MediaMasterAbstract
from dialog_machine.interfaces import ContextStorage, MsgGetter, SenderMsg, AbstractSender
from dialog_machine.view_masters import TextViewHandler, KeyBoardHandler

import logging

telegram_handler_dict = dict(text=TextViewHandler(),
                             inline_keyboard=KeyBoardHandler(),
                             keyboard=KeyBoardHandler())

""" Адаптируем функционал системы для работы с Telegram"""


class PrivateSenderError(Exception):
    """ Ошибка для внуттренних нужд """
    pass


class KeyboardMaster:
    """  Класс добавления клавиатуры """

    def __init__(self, msg_obj):
        if not isinstance(msg_obj, dict):
            return
        # Заполняем reply_markup
        try:
            keyboard_list = msg_obj['inline_keyboard']
        except KeyError:
            pass
        else:
            reply_markup = self._get_inline_keyboard(keyboard_list)
            self._reply_markup = reply_markup
            return

        try:
            keyboard_list = msg_obj['keyboard']
        except KeyError:
            reply_markup = None
        else:
            reply_markup = self._get_reply_keyboard(keyboard_list)
        self._reply_markup = reply_markup

    @staticmethod
    def _get_inline_keyboard(keyboard_list):
        # logging.debug("____--__KeyBoardList = {}".format(keyboard_list))
        keyboard = types.InlineKeyboardMarkup()
        for v in keyboard_list:
            if type(v) is list:
                row = list()
                for b in v:
                    try:
                        url = b['url']
                    except KeyError:
                        row.append(
                            types.InlineKeyboardButton(text=b.get('text', u'--'),
                                                       callback_data=b.get('callback_data', u'')))
                    else:
                        row.append(types.InlineKeyboardButton(text=b.get('text', u'--'), url=url))
                keyboard.add(*row)
            else:
                try:
                    url = v['url']
                except KeyError:
                    callback_button = types.InlineKeyboardButton(text=v.get('text', u'--'),
                                                                 callback_data=v.get('callback_data',
                                                                                     u''))
                else:
                    callback_button = types.InlineKeyboardButton(text=v.get('text', u'--'), url=url)
                keyboard.add(callback_button)
        return keyboard

    @staticmethod
    def _get_reply_keyboard(keyboard_list):
        keyboard = types.ReplyKeyboardMarkup()
        for v in keyboard_list:
            if type(v) is list:
                row = list()
                for b in v:
                    text = b.get('text', '')
                    if text != '':
                        row.append(types.KeyboardButton(text=text))
                if len(row) > 0:
                    keyboard.add(*row)
            else:
                text = v.get('text', '')
                if text != '':
                    keyboard.add(types.KeyboardButton(text=text))
        return keyboard

    def __call__(self):
        return self._reply_markup


class AbstractHandler(ABC):
    """ Потомки этого класса являются частью физического отправщика """
    _parse_mode = 'HTML'

    def __init__(self, bot):
        self._bot = bot

    @abstractmethod
    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj: KeyboardMaster):
        return None


class TextHandler(AbstractHandler):
    _parse_mode = 'HTML'

    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj: KeyboardMaster):
        # TODO: Добавить отслеживание  и обработку переполнения
        #       (text должен содержать не более 5000 символов)
        try:
            text = msg_obj['text']
            chat_id = dict_id['chat_id']
        except KeyError:
            raise PrivateSenderError()
        if text is None or text == "":
            raise PrivateSenderError()

        # Заполняем reply_markup
        reply_markup = keyboard_obj()

        state = 0
        if msg_id is None:
            try:
                msg_1 = self._bot.send_message(chat_id, text,
                                               reply_markup=reply_markup,
                                               parse_mode=self._parse_mode)
                msg_id = msg_1.message_id
                state = 1
            except ApiException as e:
                logging.debug("Text real error (chat_id, text, reply_markup, parse_mode) = {} {} {} {}".format(
                    chat_id,
                    text,
                    reply_markup,
                    self._parse_mode))
                raise SenderError('send_message ApiException: {}'.format(e))
            except KeyError:
                raise SenderError('send_message is not key=message_id')

        else:
            try:
                self._bot.edit_message_text(chat_id=chat_id,
                                            message_id=msg_id,
                                            text=text,
                                            reply_markup=reply_markup,
                                            parse_mode=self._parse_mode)
                state = 2
            except ApiException as e:
                raise SenderError(
                    'edit_message_text ApiException msg_id = {}, text={}, {}'.format(msg_id, text, e), 1)

        return msg_id, state


class PhotoHandler(AbstractHandler):
    _parse_mode = 'HTML'

    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj: KeyboardMaster):
        try:
            photo_file_id = msg_obj['photo_file_id']
            chat_id = dict_id['chat_id']
        except KeyError:
            raise PrivateSenderError()
        if photo_file_id is None:
            raise PrivateSenderError()
        text = msg_obj.get('text', None)

        # Заполняем reply_markup
        reply_markup = keyboard_obj()

        state = 0
        if msg_id is None:
            try:
                msg_2 = self._bot.send_photo(chat_id=chat_id,
                                             photo=photo_file_id,
                                             caption=text,
                                             reply_markup=reply_markup,
                                             parse_mode=self._parse_mode)
                msg_id = msg_2.message_id
                state = 1
            except ApiException as e:
                raise SenderError('send_photo ApiException, err = {}'.format(e))
            except KeyError:
                raise SenderError('send_message is not key=message_id')
        else:
            try:
                temp = types.InputMediaPhoto(photo_file_id, caption=text)
                self._bot.edit_message_media(chat_id=chat_id,
                                             message_id=msg_id,
                                             media=temp,
                                             reply_markup=reply_markup)
                state = 2
            except ApiException as e:
                raise SenderError(
                    'edit_message_media ApiException msg_id = {}, err={}'.format(msg_id, e))
        return msg_id, state


class MediaGroupHandler(AbstractHandler):
    _parse_mode = 'HTML'

    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj: KeyboardMaster):
        try:
            media_group_list = msg_obj['media_group_list']
            chat_id = dict_id['chat_id']
        except KeyError:
            raise PrivateSenderError()
        if media_group_list is None or not isinstance(media_group_list, list):
            raise PrivateSenderError()
        text = msg_obj.get('text', None)

        # Заполняем reply_markup
        reply_markup = keyboard_obj()

        state = 0
        if msg_id is None:
            try:
                t_media = list()
                for i, v in enumerate(media_group_list):
                    if i == 0 and text is not None:
                        t_media.append(types.InputMediaPhoto(media=v,
                                                             caption=text,
                                                             parse_mode=self._parse_mode))
                    else:
                        t_media.append(types.InputMediaPhoto(media=v,
                                                             parse_mode=self._parse_mode))
                msg_3 = self._bot.send_media_group(
                    chat_id=chat_id,
                    media=t_media
                )
                if isinstance(msg_3, list):
                    msg_id = [v.message_id for v in msg_3]
                else:
                    msg_id = msg_3.message_id

                state = 1
            except ApiException as e:
                raise SenderError('send_photo ApiException, err = {}'.format(e))
            except KeyError:
                raise SenderError('send_message is not key=message_id')
            # except AttributeError as e:
            #     raise SenderError(f'send_media_group msg_3={msg_3}')
            except Exception as e:
                raise SenderError(f'send_ other error  msg_3={str(e)}')
        else:
            try:
                # temp = types.InputMediaPhoto(photo_file_id, caption=text)
                if len(media_group_list) > 0:
                    t_msg_id = msg_id[0] if type(msg_id) is list else msg_id
                    self._bot.edit_message_media(chat_id=chat_id,
                                                 message_id=t_msg_id,
                                                 media=types.InputMediaPhoto(media_group_list[0]),
                                                 reply_markup=reply_markup)
                    state = 2
                else:
                    state = 0
            except ApiException as e:
                raise SenderError(
                    'edit_message_media ApiException msg_id = {}, err={}'.format(msg_id, e))
            except:
                # обработчик на всякий случай, такой ситуации теоретически произойти не должно
                raise SenderError(f'edit_message_media msg_id={msg_id}')
        return msg_id, state


class InvoiceHandler(AbstractHandler):
    """ Обработка платежа """
    _parse_mode = 'HTML'

    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj: KeyboardMaster):

        try:
            invoice_data = msg_obj['invoice']
            prices = invoice_data['price']
            currency = invoice_data['currency']
            title = invoice_data['title']
            description = invoice_data['description']
            start_parameter = invoice_data['start_parameter']
            chat_id = dict_id['chat_id']
        except KeyError:
            raise PrivateSenderError()
        except Exception:
            raise PrivateSenderError()

        is_flexible = invoice_data.get('is_flexible', False)
        photo_url = invoice_data.get('photo_url', None)
        photo_height = invoice_data.get('photo_height', None)
        photo_width = invoice_data.get('photo_width', None)
        photo_size = invoice_data.get('photo_size', None)
        need_name = invoice_data.get('need_name', None)
        need_phone_number = invoice_data.get('need_phone_number', None)
        need_email = invoice_data.get('need_email', None)
        need_shipping_address = invoice_data.get('need_shipping_address', None)
        provider_data = invoice_data.get('provider_data', None)

        reply_to_message_id = msg_obj.get('reply_to_message_id', None)

        # title, description, invoice_payload, provider_token, currency, prices,
        # start_parameter, photo_url = None, photo_size = None, photo_width = None, photo_height = None,
        # need_name = None, need_phone_number = None, need_email = None, need_shipping_address = None,
        # is_flexible = None, disable_notification = None, reply_to_message_id = None, reply_markup = None,
        # provider_data = None

        # Заполняем reply_markup
        reply_markup = keyboard_obj()

        state = 1
        if msg_id is None:
            try:
                msg_1 = self._bot.send_invoice(
                    chat_id,
                    title=title,
                    description=description,
                    invoice_payload=None,
                    provider_token=None,
                    currency=currency,
                    prices=prices,
                    start_parameter=start_parameter,
                    photo_url=photo_url,
                    photo_size=photo_size,
                    photo_width=photo_width,
                    photo_height=photo_height,
                    need_name=need_name,
                    need_phone_number=need_phone_number,
                    need_email=need_email,
                    need_shipping_address=need_shipping_address,
                    is_flexible=is_flexible,
                    disable_notification=None,
                    reply_to_message_id=reply_to_message_id,
                    reply_markup=reply_markup,
                    provider_data=provider_data)
                msg_id = msg_1.message_id
                state = 1
            except ApiException as e:
                logging.debug(f"Invoice real error (chat_id, text, reply_markup, parse_mode) = "
                              f"{chat_id} {title} {reply_markup} {self._parse_mode}")
                raise SenderError(f'send_invoice ApiException: {e}')
            except KeyError:
                raise SenderError('send_invoice is not key=message_id')

        # else:
        #     try:
        #         self._bot.edit_message_text(chat_id=chat_id,
        #                                     message_id=msg_id,
        #                                     text=text,
        #                                     reply_markup=reply_markup,
        #                                     parse_mode=self._parse_mode)
        #         state = 2
        #     except ApiException as e:
        #         raise SenderError(
        #             'edit_message_text ApiException msg_id = {}, text={}, {}'.format(msg_id, text, e))

        return msg_id, state


class ShippingHandler(AbstractHandler):
    """ Обработка доставки """
    _parse_mode = 'HTML'

    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj: KeyboardMaster):

        try:
            shipping_data = msg_obj['shipping']
        except KeyError:
            raise PrivateSenderError()
        except Exception:
            raise PrivateSenderError()

        try:
            shipping_query_id = shipping_data['shipping_query_id']
            ok = shipping_data['ok']
            shipping_options = shipping_data['shipping_options']
            error_message = shipping_data['error_message']
        except KeyError as e:
            SenderError(f'Problems with format SenderError e={e}')
        else:
            try:
                self._bot.answer_shipping_query(
                    shipping_query_id,
                    ok=ok,
                    shipping_options=shipping_options,
                    error_message=error_message)
            except ApiException as e:
                logging.debug(f"Shipping_query real error (shipping_query_id, shipping_options, parse_mode) = "
                              f"{shipping_query_id} {shipping_options} {self._parse_mode}")
                raise SenderError(f'answer_shipping_query ApiException: {e}')
            except KeyError:
                raise SenderError('answer_shipping_query')

            state = 1
            return msg_id, state


class PreCheckoutHandler(AbstractHandler):
    """ Обработка предобработки оплаты """
    _parse_mode = 'HTML'

    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj: KeyboardMaster):

        try:
            pre_checkout = msg_obj['pre_checkout']
        except KeyError:
            raise PrivateSenderError()
        except Exception:
            raise PrivateSenderError()

        try:
            pre_checkout_query_id = pre_checkout['pre_checkout_query_id']
            ok = pre_checkout['ok']
            error_message = pre_checkout['error_message']
        except KeyError as e:
            SenderError(f'Problems with format SenderError e={e}')
        else:
            try:
                self._bot.answer_pre_checkout_query(
                    pre_checkout_query_id,
                    ok=ok,
                    error_message=error_message)
            except ApiException as e:
                logging.debug(f"Shipping_query real error (pre_checkout_query_id, parse_mode) = "
                              f"{pre_checkout_query_id} {self._parse_mode}")
                raise SenderError(f'pre_checkout_query ApiException: {e}')
            except KeyError:
                raise SenderError('pre_checkout_query')

            state = 1
            return msg_id, state


class DeleterHandler(AbstractHandler):

    def __call__(self, dict_id, msg_id, msg_obj, keyboard_obj):
        if (msg_obj is None or len(msg_obj) == 0 or msg_obj == dict(text='')) \
                and isinstance(dict_id, dict):
            if msg_id is not None:
                try:
                    chat_id = dict_id['chat_id']
                except KeyError:
                    raise PrivateSenderError()
                try:
                    return self._bot.delete_message(chat_id=chat_id, message_id=msg_id), 3
                except Exception as e:
                    raise SenderError(
                        'delete_message ApiException msg_id = {}, err={}'.format(msg_id, e))
            else:
                return None, 0
        else:
            raise PrivateSenderError()


class SenderTelegram(AbstractSender):
    """ Класс отвечает за физическую отправку сообщения и получения его id  """

    _parse_mode = 'HTML'

    def __init__(self, bot):
        assert isinstance(bot, TeleBot)
        self._handlers_list = list()
        self._handlers_list.append(DeleterHandler(bot))
        self._handlers_list.append(InvoiceHandler(bot))
        self._handlers_list.append(ShippingHandler(bot))
        self._handlers_list.append(PreCheckoutHandler(bot))
        self._handlers_list.append(MediaGroupHandler(bot))
        self._handlers_list.append(PhotoHandler(bot))
        self._handlers_list.append(TextHandler(bot))

    def __call__(self, dict_id, msg_id, msg_obj):
        """ Метод выполняет роль супервайзера и распределяет задания вывода сообщения
        между различными специализированными методами (вывод текста, вывод фото ...) """

        logging.debug("_|_ Try to send Message: {}; {}\n {}".format(dict_id, msg_id, msg_obj))
        keyboard_obj = KeyboardMaster(msg_obj)
        for v in self._handlers_list:
            try:
                return v(dict_id, msg_id, msg_obj, keyboard_obj)
            except PrivateSenderError:
                continue
        return None, 0


class EnvironmentTelegram:
    """ Класс объекта доступа к окружению """

    def __init__(self, bot: TeleBot, dict_id=None, default_dict=None, msg_getter=None):
        self._bot = bot
        self._dict_id = dict_id if isinstance(dict_id, dict) else dict()
        self._default_dict = default_dict
        self._msg_getter = msg_getter

    def __getitem__(self, key):
        if key == '_get_me':
            return self._bot.get_me()
        elif key == '_get_chat':
            return self._bot.get_chat(self._dict_id['chat_id'])
        elif key == '_get_chat_administrators':
            return self._bot.get_chat_administrators(self._dict_id['chat_id'])
        elif key == '_get_chat_member':
            return self._bot.get_chat_member(self._dict_id['chat_id'], self._dict_id['user_id'])
        elif key == '_get_chat_administrators':
            return self._bot.get_chat_members_count(self._dict_id['chat_id'])
        elif key == '_dict_id':
            return self._dict_id
        elif type(key) is dict:  # == '_dict_id_by_alias':
            try:
                _alias_msg = key['_dict_id_by_alias']
            except KeyError:
                pass
            else:
                return self._msg_getter.get_dict_id_by_alias(_alias_msg)

            raise KeyError('EnvironmentTelegram')
        else:
            if self._default_dict is not None:
                return self._default_dict[key]
            else:
                raise KeyError('EnvironmentTelegram')


class UserInput:
    _param_def = dict()

    def __init__(self, bot: TeleBot, call=None, message=None, param=None,
                 pre_checkout_query=None, shipping_query=None):
        self._bot = bot
        self._call = call
        self._message = message
        self._pre_checkout_query = pre_checkout_query
        self._shipping_query = shipping_query
        self._param = param if isinstance(param, dict) else self._param_def

    def add_call(self, call):
        self._call = call
        return self

    def add_message(self, message):
        self._message = message
        return self

    def add_param(self, param):
        self._param = param
        return self

    def add_pre_checkout_query(self, pre_checkout_query):
        self._pre_checkout_query = pre_checkout_query
        return self

    def add_shipping_query(self, shipping_query):
        self._shipping_query = shipping_query
        return self

    def _get_message_dict(self, message):
        result = dict()
        try:
            result['text'] = message.text
        except AttributeError:
            result['text'] = message.caption
        except:
            return result
        if message.content_type == 'photo':
            try:
                result['photo'] = [v.file_id for v in message.photo]
            except:
                logging.warning('False format of message')
        result['message'] = message
        return result

    def __getattr__(self, key):
        return self._get_by_key(key)

    def __getitem__(self, key):
        try:
            return self._get_by_key(key)
        except:
            return None

    def _get_by_key(self, key):
        if key == 'callback' or key == 'call_data':
            return self._call.data if self._call is not None else None
        elif key == 'call':
            return self._call
        elif key == 'input_message':
            return self._get_message_dict(self._message)
        elif key == 'call_message':
            return self._get_message_dict(self._call.message)
        elif key == 'message':
            if self._message is not None:
                return self._get_message_dict(self._message)
            else:
                return self._get_message_dict(self._call.message)
        elif key == 'shipping_query':
            return self._shipping_query
        elif key == 'pre_checkout_query':
            return self._pre_checkout_query
        elif key in set(self._param.keys()):
            return self._param[key]
        else:
            raise KeyError('Unknown key in ParamObj: "{}"'.format(key))


class MediaMaster(MediaMasterAbstract):
    """ Класс объекта, определяющего физический доступ
        к медиа данным по file_id (к физическим картинкам, видео, аудио ...) """

    def __init__(self, bot):
        self._bot = bot

    def get_file_id(self, file):
        """ Возвращает id по файлу """
        return None

    def get_file(self, file_id, filter_type=None):
        """ Возвращает файл по id  """
        return self._bot.get_file(file_id)


class DialogMachine:
    """ Адаптер для DialogMachineCore под телеграм """
    _def_dict = dict()

    def __init__(self, bot, context_storage, state_machine, msg_getter, sender_msg, msg_obj_default) -> object:
        """
            bot - объект TeleBot
            context_storage - объект, реализующий хранение текущего состояния
            sender_msg - объект, отвечающий за отправку сообщений
            msg_getter - хранение информации для этих сообщений
            state_machine - объект-хранитель графа состояния
            msg_obj_default - объект сообщения по умолчанию (шаблон сообщения со всеми
                                ключевыми полями. которые будет использоваться)
        """
        assert isinstance(bot, TeleBot)
        assert isinstance(context_storage, ContextStorage)
        assert isinstance(state_machine, StateMachine)
        assert isinstance(msg_getter, MsgGetter)
        assert isinstance(sender_msg, SenderMsg)
        assert isinstance(msg_obj_default, dict)

        # Собираем dialog_machine
        self._bot = bot
        sender = SenderTelegram(bot)
        sender_msg.set_sender(sender)
        media_master = MediaMaster(bot)
        self._msg_getter = msg_getter
        self._dialog_machine_core = DialogMachineCore(context_storage, state_machine,
                                                      msg_getter, sender_msg,
                                                      msg_obj_default, media_master=media_master)

    def process_start(self, alias_msg, message, env_data, flag_of_context=True):
        """ flag_of_context - флаг показывает откуда брать контекст (True - из входящего сообщения message
                              False - из сообщения alias_msg) """
        dict_id = dict(chat_id=message.chat.id, user_id=message.from_user.id)
        main_dict_id = dict_id if alias_msg is None or flag_of_context else None
        env_ = EnvironmentTelegram(self._bot, dict_id, env_data, msg_getter=self._msg_getter)
        param_ = UserInput(self._bot, message=message)
        self._dialog_machine_core.process(main_dict_id, None, alias_msg, '/start', param_, env_)

    def process_command(self, command, message, env_data):
        dict_id = dict(chat_id=message.chat.id, user_id=message.from_user.id)
        env_ = EnvironmentTelegram(self._bot, dict_id, env_data, msg_getter=self._msg_getter)
        param_ = UserInput(self._bot, message=message)
        self._dialog_machine_core.process(dict_id, None, None, command, param_, env_)

    def process_command_not_by_msg(self, command, dict_id: dict, env_data):
        """ Команда вызываемая не в результате прихода сообщения,
        а в результате другого события (пользовательского) """
        env_ = EnvironmentTelegram(self._bot, dict_id, env_data, msg_getter=self._msg_getter)
        param_ = self._def_dict
        self._dialog_machine_core.process(dict_id, None, None, command, param_, env_)

    def process_msg(self, message, env_data, extra_param=None):
        """ При вызове сообщения
            message -  данные сообщения пользователя
            env_data - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        dict_id = dict(chat_id=message.chat.id, user_id=message.from_user.id)
        env_ = EnvironmentTelegram(self._bot, dict_id, env_data, msg_getter=self._msg_getter)
        param_ = UserInput(self._bot, message=message, param=extra_param)
        self._dialog_machine_core.process(dict_id, None, None, None, param_, env_)

    def process_callback(self, call, env_data):
        """ При обработке callback от сообщения
            call -  (callback_data) параметры при callback, если есть
            env_data - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        dict_id = dict(chat_id=call.message.chat.id, user_id=call.from_user.id)
        msg_id = call.message.message_id
        env_ = EnvironmentTelegram(self._bot, dict_id, env_data, msg_getter=self._msg_getter)
        param_ = UserInput(self._bot, call=call)
        self._dialog_machine_core.process(dict_id, msg_id, None, call.data, param_, env_)

    # методы для оплаты в телеграмм

    def process_shipping(self, command, shipping_query, env_data):
        """ При обработке shipping_query
            command - команда для вызова соответствующей вершины
            shipping_query -  данные о доставке
            env_data - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        dict_id = None
        alias_msg = shipping_query.invoice_payload
        env_ = EnvironmentTelegram(self._bot, dict_id, env_data, msg_getter=self._msg_getter)
        param_ = UserInput(self._bot, shipping_query=shipping_query)
        self._dialog_machine_core.process(dict_id, None, alias_msg, command, param_, env_)

    def process_pre_checkout(self, command, pre_checkout_query, env_data):
        """ При обработке pre_checkout
            command - команда для вызова соответствующей вершины
            pre_checkout_query -  данные о предстоящей оплате
            env_data - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        dict_id = None
        alias_msg = pre_checkout_query.invoice_payload
        env_ = EnvironmentTelegram(self._bot, dict_id, env_data, msg_getter=self._msg_getter)
        param_ = UserInput(self._bot, pre_checkout_query=pre_checkout_query)
        self._dialog_machine_core.process(dict_id, None, alias_msg, command, param_, env_)


logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s] %(message)s', level=logging.WARNING)
