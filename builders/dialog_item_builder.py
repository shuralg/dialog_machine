#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

from dialog_machine.builders.base_builders import *
from dialog_machine.dialog_machine_core import *
from dialog_machine.session_master import DataItemConnectorBuilder

from dialog_machine.view_masters import ComplexViewMaster

""" Билдер для взаимодействия с таблицей.
    Реализуем базовый функционал вставки (insert)
"""


class SimpleVertexControllerMaster(ComplexVertexControllerMaster):
    """ Простейший контроллер вершини. который переходит на первую же вершину списка"""

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, additional_args: dict, vertex_context_dict: dict):
        try:
            item = self._vertex_list[0]
        except IndexError:
            return None, dict()
        else:
            try:
                return item['vertex_name'], item['control_dict']
            except KeyError:
                return None, dict()


# class DictSwitcherForDialogItem(DictSwitcher):
#
#     def __call__(self, session_manager, user_input=None, vert_control_dict=None,
#                  env_dict=None, session_dict=None):
#         result = dict()
#
#         if vert_control_dict is not None:
#             for k, v in vert_control_dict.items():
#                 result[k] = deepcopy(v)
#
#         if user_input is not None:
#             for k in self._user_input_key:
#                 try:
#                     result[k] = deepcopy(user_input[k])
#                 except KeyError:
#                     pass
#
#         return result


class DialogItemControllerUpd(ComplexControllerMaster):

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        """ Вычищаем все управляющие словари для сообщения
        :param vertex_context_dict:
        """
        common_control_dict.clear()
        msg_control_dict.clear()
        return dataitem_protect, common_control_dict, msg_control_dict


class ViewMasterDialogItemUpd(ComplexViewMaster):

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        in_view['inline_keyboard'] = list()
        return in_view


# class DialogItemDictSwitcherUpd(DictSwitcher):
#     """ формирует входные данные для потомков AbstractDialogItemVertexControllerMasterUpd """
#
#     def __call__(self, session_manager, user_input=None, vert_control_dict=None,
#                  env_dict=None, session_dict=None):
#         result = dict()
#
#         result['control_dict'] = deepcopy(vert_control_dict) if vert_control_dict is not None else None
#
#         result['user_input'] = deepcopy(user_input) if user_input is not None else None
#
#         return result


class DialogItemDictSwitcher(DictSwitcherVB):

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        if vert_control_dict is not None:
            return deepcopy(vert_control_dict)
        else:
            return dict()

# Билдер

class DialogItemBuilder(BaseBuilder):
    """ Базовая единица диалога: вершина, формирующая вопрос + вершина обрабатывающая ответ пользователя """

    # _suffix = '_response'
    # _upd_suf = '_updater'

    def __init__(self, vertex_name: str, vertex_blob_request: VertexBlobBuilder,
                 next_vertex_name: str, except_vertex_name: str,
                 vertex_controller: ComplexVertexControllerMaster,
                 vertex_blob_response: VertexBlobBuilder,
                 data_item_name: str, vertex_name_response: str, vertex_upd_name: str,
                 parent_data_item_name=None):
        super().__init__()
        self._vertex_name = vertex_name
        self._vertex_blob_request = vertex_blob_request
        self._vertex_blob_response = vertex_blob_response
        self._vertex_controller = vertex_controller
        self._data_item_name = data_item_name
        self._parent_data_item_name = parent_data_item_name
        self._next_vertex_name = next_vertex_name
        self._except_vertex_name = except_vertex_name
        self._vertex_upd_name = vertex_upd_name
        self._vertex_name_response = vertex_name_response

    def build_a(self, vertex_dict):
        vertex_upd_name = self._vertex_upd_name
        # vertex_upd_name - вершина должна удалять inline клавиатуру из сообщения
        vertex_name_response = self._vertex_name_response

        vertex_controller_request = SimpleVertexControllerMaster(
            [dict(vertex_name=vertex_upd_name, control_dict=dict())])
        self._vertex_blob_request.data_item(self._data_item_name)
        self._vertex_blob_request.parent_data_item(self._parent_data_item_name)
        vertex_request_builder = VertexBuilder(vert_blob_list=[self._vertex_blob_request.build()],
                                               dict_id_getter=None,
                                               wait_signal_setter=None,
                                               controller_vert_keeper=vertex_controller_request)

        # Работаем над вершиной для обновления (updater_vertex)
        # Задача vertex_blob убрать клавиатуру и обнулить управляющие данные
        upd_vertex_blob_builder = VertexBlobBuilder(state_keeper=DataItemConnectorBuilder(None, None),
                                                    view_keeper=ViewMasterDialogItemUpd(template=None),
                                                    controller_keeper=DialogItemControllerUpd(list()))

        self._vertex_controller.set_param(vertex_list=[dict(vertex_name=vertex_name_response, control_dict=dict()),
                                                       dict(vertex_name=self._except_vertex_name, control_dict=dict())])
        upd_vertex_builder = VertexBuilder(vert_blob_list=[upd_vertex_blob_builder.build()],
                                           wait_signal_bool=False,
                                           controller_vert_keeper=self._vertex_controller)

        # Работаем над вершиной ответа vertex_response
        self._vertex_blob_response.data_item(self._data_item_name)
        self._vertex_blob_response.parent_data_item(self._data_item_name)

        control_dict_switch = DialogItemDictSwitcher()
        self._vertex_blob_response.control_dict_switch(control_dict_switch)

        vertex_controller_response = SimpleVertexControllerMaster(
            [dict(vertex_name=self._next_vertex_name, control_dict=dict())])
        vertex_response_builder = VertexBuilder(vert_blob_list=[self._vertex_blob_response.build()],
                                                dict_id_getter=None,
                                                wait_signal_setter=None,
                                                wait_signal_bool=False,
                                                controller_vert_keeper=vertex_controller_response)

        self._vertex_blob_response.data_item(self._data_item_name)
        self._vertex_blob_response.parent_data_item(self._data_item_name)

        # Перезаписываем вершины (удаляем, а потом вставляем)
        for v in [self._vertex_name, vertex_upd_name, vertex_name_response]:
            try:
                del(vertex_dict[v])
            except KeyError:
                pass
        vertex_dict[self._vertex_name] = vertex_request_builder.build()
        vertex_dict[vertex_upd_name] = upd_vertex_builder.build()
        vertex_dict[vertex_name_response] = vertex_response_builder.build()

        return vertex_dict
