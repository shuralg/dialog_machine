#!/usr/bin/python3.7
# -*- coding: utf-8 -*-
import logging
from copy import deepcopy

from jinja2 import Environment

from dialog_machine.dialog_machine_core import BaseElement, ViewMaster


class ViewSwitch(BaseElement):
    """ Класс коммутатора для View.
        Объект принимает на вход входное представление (input view) и генерируемое представление (generated view)
        отрабатывает логику компоновки этих представлений и выдает результирующее View
    """

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, in_view, gen_view, additional_args: dict, vertex_context_dict: dict):
        return gen_view


class ViewHandler:
    """ Обработчик шаблона сообщения """

    def get_param(self):
        return dict()

    def __call__(self, template, arg, additional_args=None):
        return template


class TextViewHandler(ViewHandler):
    """
        Обработчик текстовой части шаблона
        Должен быть перенесен в отдельный файл посвященный локализации под Telegram
    """
    _add_arg_def = dict()

    # TODO: Обработчик текстовой части шаблона
    #       Должен быть перенесен в отдельный файл посвященный локализации под Telegram

    def __init__(self):
        self._env = Environment()

    def __call__(self, template, arg, additional_args=None):
        t = self._env.from_string(template)
        if additional_args is not None:
            return t.render(arg=arg, add_arg=additional_args)
        else:
            return t.render(arg=arg, add_arg=self._add_arg_def)


class KeyBoardHandler(ViewHandler):
    """
        Обработчик части шаблона для формирования клавиатур
        Должен быть перенесен в отдельный файл посвященный локализации под Telegram
    """

    # TODO: Обработчик части шаблона для формирования клавиатур
    #       Должен быть перенесен в отдельный файл посвященный локализации под Telegram

    _def_dict = dict()
    _dict_name = 'key_board_dict'
    _callback = 'callback_data'

    def __init__(self, dict_name='key_board_dict', callback_name='callback_data'):
        self._dict_name = dict_name
        self._callback = callback_name

    def get_param(self):
        return dict(dict_name=self._dict_name, callback_name=self._callback)

    def _get_el(self, temp_el, arg_el):
        try:
            callback_data = arg_el[self._callback]
        except KeyError:
            return dict(text=temp_el['text'], url=temp_el['url'])
        else:
            return dict(text=temp_el['text'], callback_data=callback_data)

    def __call__(self, template, arg, additional_args=None):
        try:
            arg_d = arg[self._dict_name]
        except KeyError:
            return None
        else:
            assert isinstance(arg_d, dict) or arg_d is None
            assert isinstance(template, list)
            result = list()
            try:
                for list_ in template:
                    if isinstance(list_, list):
                        t_list = list()
                        for ll in list_:
                            t_list.clear()
                            try:
                                arg_v = arg_d[ll['id']]
                            except KeyError:
                                arg_v = self._def_dict
                            try:
                                t_list.append(self._get_el(ll, arg_v))
                            except KeyError:
                                pass
                        if len(t_list) > 0:
                            result.append(t_list)
                    else:
                        try:
                            arg_v = arg_d[list_['id']]
                        except KeyError:
                            arg_v = self._def_dict
                        try:
                            result.append(self._get_el(list_, arg_v))
                        except KeyError:
                            pass
            except Exception:
                return None
            return result


class ComplexViewMaster(ViewMaster):
    """ Класс отвечает за формирование непосредственно сообщения из шаблона
        А также объект принимает на вход входное представление (input view) и
        отрабатывает логику компоновки этих представлений и выдает результирующее View"""

    _default_handler_dict = dict(text=TextViewHandler(),
                                 inline_keyboard=KeyBoardHandler(),
                                 keyboard=KeyBoardHandler())
    _default_view_switch = ViewSwitch()

    def __init__(self, template, view_switch=None, handlers_dict=None, message_default=None, **args):
        """
            template - словарь шаблонов элементов сообщения (шаблона текста сообщения, клавиатуры, инлайн клавиатуры,
                                                                набора аудио, видео .....)
            view_switch - объект-коммутатор, обеспечивающий слияние представления полученного на предыдущих шагах и
                            полученного в результате обработки шаблона
            handlers_dict - словарь обработчиков, каждый элемент которого соответствует обработчику того или иного
                            элемента шаблона (имена элементов  )
            message_default - словарь элементов сообщения по умолчанию (текст по умолчанию, клавиатура по умолчанию ...)
        """
        super().__init__(**args)
        self._template = template
        if view_switch is None:
            self._view_switch = self._default_view_switch
        else:
            assert isinstance(view_switch, ViewSwitch)
            self._view_switch = view_switch

        if handlers_dict is None:
            self._handlers_dict = self._default_handler_dict
        else:
            assert isinstance(handlers_dict, dict)
            for v in handlers_dict.values():
                assert isinstance(v, ViewHandler)
            self._handlers_dict = handlers_dict

        if message_default is None:
            self._message_default = dict()
        else:
            assert isinstance(message_default, dict)
            self._message_default = message_default

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        if env_dict is not None:
            for k in self._env_keys:
                try:
                    result[k] = deepcopy(env_dict[k])
                except KeyError:
                    pass
        return result

    def get_param(self, key_handler):
        try:
            h = self._handlers_dict[key_handler]
        except KeyError:
            return dict()
        return h.get_param()

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        result = dict()
        logging.debug(" ____ViewMaster___template: {}\n\n{}".format(self._template, dataitem_protect))
        if self._handlers_dict is not None:
            for k, v in self._template.items():
                try:
                    handler_item = self._handlers_dict[k]
                except KeyError:
                    pass
                else:
                    result[k] = handler_item(v, dataitem_protect, additional_args)

        # Вставляем дефолтные элементы, если они не были созданы
        for k, v in self._message_default.items():
            if k not in result:
                result[k] = v

        return self._view_switch.main(in_view, result, additional_args=additional_args,
                                      vertex_context_dict=vertex_context_dict)