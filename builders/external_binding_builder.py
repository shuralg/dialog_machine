#!/usr/bin/python3.7
# -*- coding: utf-8 -*-

from dialog_machine.builders.base_builders import *

from dialog_machine.dialog_machine_core import VertexBlob, Vertex


# from view_masters import ComplexViewMaster


class ExternalBindingController(PrototypeControllerMaster):
    """ Контроллер для внешнего связывания """

    def __init__(self, vertex_name=None, control_dict_d=None, command='/start', **kwargs):
        """
        :param vertex_name: - имя вершины-обработчика
        :param control_dict_d: - управляющие данные по умолчанию
        :param command: - команда (обычно /start)
        :param kwargs:
        """
        super().__init__()
        self._vertex_name = vertex_name
        self._control_dict_d = control_dict_d if control_dict_d is not None else dict()
        self.command = command

    def set_param(self, vertex_name=None, control_dict_d=None, command=None):
        if vertex_name is not None:
            self._vertex_name = vertex_name
        if control_dict_d is not None:
            self._control_dict_d = control_dict_d
        if command is not None:
            self.command = command

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return self._control_dict_d

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        """
        Основной метод выполнения объекта данного класса
        Обеспечивает реализацию основной логики контроллера.
        Возвращает common_control_dict (без привязки к конкретному сообщению)
            и msg_control_dict (словарь команд с привязкой к сообщению) для соответствующего DataItem
        В процессе выполнения модифицирует входящий dataitem_protect
        :param vertex_context_dict:
        """
        msg_control_dict[self.command] = dict(vertex_name=self._vertex_name,
                                              control_dict=additional_args)
        return dataitem_protect, common_control_dict, msg_control_dict


class DictID_GetterForGroupMsg(DictIdGetter):
    """ Для отправки сообщения в подключенную группу """

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return deepcopy(env_dict['_dict_id'])

    def main(self, dict_id_in, msg_id_in, additional_args: dict, vertex_context_dict: dict):
        """ dict_id_in - адресный словарь,
            msg_id_in - id сообщения,
            additional_args - словарь аргументов. полученный с помощью ControlDictSwitch
            :param vertex_context_dict:
        """
        dict_id, msg_id = additional_args, None
        return dict_id, msg_id


class ExternalBindingViewMaster(ViewMaster):
    _link = "https://t.me/{bot_name}?{command}={param}"
    _link_for_text = "{param}"

    def __init__(self, button_caption, command='/start', link_template=None,
                 text_replay_link=None):
        """
        :param button_caption: подпись кнопки перехода
        :param command: команда /start или /startgroup
        :param link_template: шаблон ссылки, по-умолчанию 
                            _link = 'https://t.me/{bot_name}?{command}={param}'
        :param text_replay_link: подстрока на которую будет заменена ссылка 
        """
        super().__init__()
        self._template = [[dict(text=button_caption)]]
        self._button_caption = button_caption
        self._command = command.replace('/', '')
        if link_template is not None:
            self._link = link_template
        self._text_replay_link = text_replay_link

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        get_me = env_dict['_get_me']
        bot_name = get_me.username
        return dict(param=alias_msg, bot_name=bot_name, command=self._command)

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        # [[dict(text=self., callback_data=callback_data)]]
        callback_data = self._link.format(**additional_args)
        res = list()
        for v in self._template:
            if isinstance(v, list):
                ins_list = list()
                for vv in v:
                    ins_list.append(dict(text=vv.get('text', ''), url=callback_data))
                res.append(ins_list)
            elif isinstance(v, dict):
                res.append(dict(text=v.get('text', ''), url=callback_data))

        try:
            keyboard = in_view['inline_keyboard']
        except KeyError:
            keyboard = res
        else:
            if isinstance(keyboard, list):
                keyboard.extend(res)
        in_view['inline_keyboard'] = keyboard

        if self._text_replay_link is not None:
            ll = self._link_for_text.format(**additional_args)
            _text = in_view['text']
            in_view['text'] = _text.replace(self._text_replay_link, ll)
        # заменяем в тексте на ссылку

        return in_view


class ExternalBindingCoreBuilder(BaseBuilder):
    """ Билдер для организации внешнего связывания"""
    _SUF_HANDLER_NAME = 'external_binding_handler'

    def __init__(self, vertex_name, controller: ExternalBindingController, button_caption: str,
                 vertex_handler: Vertex, is_startgroup=False, text_replay_link=None):
        """ controller - контроллер, создает записи в msg_dict для команды '/start',
                            которая задействована в механизме внешнего связывания
            vertex_name - имя вершины, которая будет реализовывать переход в механизме
                            внешнего связывания (функционал внешнего связывания будет присоединен
                            к уже готовой вершине)
            vertex_handler - вершина, обработчик внешнего перехода
            is_startgroup - флаг, показывающий переход ли это start или startgroup
            text_replay_link - ссылка для замены в тексте сообщения на ссылку для вставки
        """
        super().__init__()
        self._vertex_name = vertex_name
        self._controller = controller
        self._button_caption = button_caption
        self._vertex_handler = vertex_handler
        self._is_startgroup = is_startgroup
        self._text_replay_link = text_replay_link

    def build_a(self, vertex_dict):
        # Создаем vertex_blob
        try:
            sender_vertex = vertex_dict[self._vertex_name]
        except KeyError:
            pass
        else:
            vertex_handler_name = '{}_{}'.format(self._vertex_name, self._SUF_HANDLER_NAME)
            vertex_dict[vertex_handler_name] = self._vertex_handler
            self._controller.set_param(vertex_name=vertex_handler_name)

            command = '/startgroup' if self._is_startgroup else '/start'
            vertex_blob = VertexBlob(
                view_keeper=ExternalBindingViewMaster(
                    button_caption=self._button_caption,
                    command=command,
                    text_replay_link=self._text_replay_link
                ),
                controller_keeper=self._controller
            )
            sender_vertex.update_vert(vert_blob_list=[vertex_blob])

        return vertex_dict
