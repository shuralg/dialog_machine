#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

""" Реализация билдера для inline-меню """

from dialog_machine.builders.base_builders import *
from dialog_machine.dialog_machine_core import *
from dialog_machine.session_master import DataItemConnectorBuilder
import logging

from dialog_machine.view_masters import ViewSwitch, ComplexViewMaster

ITEM_NUM = 'item_num'

EMPTY_VIEW_MASTER = ComplexViewMaster(template=dict(),
                                      handlers_dict=dict())


class ChoiceInlineMenuController(ComplexControllerMaster):
    _dict_name = 'key_board_dict'
    _vert_name_h = 'item_number'

    # TODO: необходимо обеспечить, чтоб этот _dict_name и _dict_name объекта KeyBoardHandler были синхронизированы

    def set_param(self, dict_name=None, **args):
        """
            dict_name - имя параметра, в котором хранится список для клавиатуры
        """
        super(ChoiceInlineMenuController, self).set_param(**args)

        if dict_name is not None:
            assert type(dict_name) is str

            self._dict_name = dict_name

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        """
        Преобразует self._vertex_list в список элементов представления и управляющие словари msg_control_dict

        Основной метод выполнения объекта данного класса
        Обеспечивает реализацию основной логики контроллера.
        Возвращает common_control_dict (без привязки к конкретному сообщению)
            и msg_control_dict (словарь команд с привязкой к сообщению) для соответствующего DataItem
        В процессе выполнения модифицирует входящий dataitem_protect
        :param vertex_context_dict:
        """
        name_def, id_f = 'cont_param_{}', 1
        result = dict()
        logging.debug("___Controller Master: {}".format(self._vertex_list))
        for id, v in enumerate(self._vertex_list):
            field_name = name_def.format(id)

            result[id] = dict(callback_data=field_name, text=v.get('field_caption', '-'))
            # temp_cd = dict(vertex_name=v['vertex_name'], control_dict=v['control_dict'])
            msg_control_dict[field_name] = dict(vertex_name=v['vertex_name'], control_dict=v['control_dict'])

        dataitem_protect[self._dict_name] = result
        return dataitem_protect, common_control_dict, msg_control_dict


# Классы для вершины-обработчика сообщения (updater)
class ChoiseMenuDictSwitcher(DictSwitcherVB):
    """ Switcher для Контроллера вершины в вершине обработки сообщения
        Оставляем только vert_control_dict"""

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):

        """  Основной метод, в котором осуществляется коммутация
             user_input - данные введеные пользователем
             # control_dict - Управляющие данные сформированные Контроллером ранее при обработки этого блока
             vert_control_dict - управляющие данные вершины сформированные предыдущим Контроллером вершины
                    (потомки класса AbstractVertexControllerMaster)
             env_dict - данные окружения (имя бота, информация о боте ...)
             session_dict - данные из dataitem (актуально для VertexBlob)
             :param alias_msg:
        """
        result = dict()

        if vert_control_dict is not None:
            for k, v in vert_control_dict.items():
                result[k] = deepcopy(v)

        return result


class DeleterControllerMaster(ComplexControllerMaster):
    """ Класс контроллера обнуляющий данные сообщения common_control_dict и msg_control_dict """

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        return result

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
        return dataitem_protect, dict(), dict()


class UpdaterMsgVertexControllerMaster(ComplexVertexControllerMaster):
    """ Контроллер вершины для перехода в нужную """

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        if vert_control_dict is not None:
            for k, v in vert_control_dict.items():
                result[k] = deepcopy(v)

        return result

    def main(self, additional_args: dict, vertex_context_dict: dict):
        """ Метод реализует основной функционал класса
        :param additional_args:
        :param vertex_context_dict:
        """
        try:
            i = additional_args[ITEM_NUM]
        except KeyError:
            return None, dict()
        else:
            try:
                temp_cd = self._vertex_list[i]
            except IndexError:
                return None, dict()
            else:
                control_dict = temp_cd.get('control_dict', dict())
                vertex_name = temp_cd.get('vertex_name', None)
                return vertex_name, control_dict


class ViewSwitchChoiceMenu(ViewSwitch):

    def __init__(self, default_view):
        assert isinstance(default_view, dict)
        super(ViewSwitchChoiceMenu, self).__init__()
        self._default_view = default_view

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, in_view, gen_view, additional_args: dict, vertex_context_dict: dict):
        res = in_view if in_view is not None else dict()
        for k, v in gen_view.items():
            res[k] = v
        for k, v in self._default_view.items():
            if not k in res:
                res[k] = v
        return res


# Билдер

class ChoiseMenuBuilder(BaseBuilder):
    _default_msg = dict(text='Привет! Мир!!! Это тестовое сообщение с меню')
    _main_vertex_name = 'choise_menu_vert'
    _suf_name = '_hidden_vert'

    def __init__(self, vertex_name, handlers_dict, menu_list, msg_text=None,
                 update_msg_text=None, update_vertex_blob=None,
                 default_msg=None, suf_name=None):
        """ menu_list - вложенный список, отражающий структуру меню """
        assert type(vertex_name) is str
        assert isinstance(menu_list, list)
        assert isinstance(msg_text, str) or msg_text is None
        assert isinstance(default_msg, dict) or default_msg is None
        assert isinstance(suf_name, dict) or suf_name is None
        assert isinstance(handlers_dict, dict)
        assert isinstance(update_msg_text, str) or update_msg_text is None
        assert isinstance(update_vertex_blob, VertexBlob) or update_vertex_blob is None
        super().__init__()
        self._vertex_name = vertex_name
        self._menu_list = menu_list
        self._default_msg = default_msg
        self._msg_text = msg_text
        self._handlers_dict = handlers_dict
        self._update_msg_text = update_msg_text
        self._update_vertex_blob = update_vertex_blob
        if suf_name is not None:
            self._suf_name = suf_name

    def _get_keyboard(self):
        view_res, vert_list = list(), list()
        id = 0
        for v in self._menu_list:
            if isinstance(v, list):
                res1 = list()
                for vv in v:
                    try:
                        url = vv['url']
                    except KeyError:
                        try:
                            vl_item = dict(vertex_name=vv['vertex_name'], control_dict=vv['control_dict'])
                        except KeyError:
                            continue
                        else:
                            vert_list.append(vl_item)
                            res1.append(dict(id=id, text=vv.get('text', '-')))
                            id += 1
                    else:
                        res1.append(dict(url=url, text=vv.get('text', '-')))
                view_res.append(res1)
            elif isinstance(v, dict):
                try:
                    url = v['url']
                except KeyError:
                    try:
                        vl_item = dict(vertex_name=v['vertex_name'], control_dict=v['control_dict'])
                    except KeyError:
                        continue
                    else:
                        vert_list.append(vl_item)
                        view_res.append(dict(id=id, text=v.get('text', '-')))
                        id += 1
                else:
                    view_res.append(dict(url=url, text=v.get('text', '-')))
        return view_res, vert_list

    def build_a(self, vertex_dict):
        if vertex_dict is None:
            vertex_dict = dict()
            vertex_dict[self._vertex_name] = None

        # поиск нужного элемента
        try:
            vertex = vertex_dict[self._vertex_name]
        except KeyError:
            pass
        else:
            # Сначала ищем соответствующую вершину (нам нужно ее не найти, иначе создаем новую)
            temp_name = vertex_updater_name = self._vertex_name + self._suf_name
            i = 1
            while True:
                try:
                    vertex_update = vertex_dict[temp_name]
                except KeyError:
                    vertex_updater_name = temp_name
                    break
                else:
                    temp_name = f'{vertex_updater_name}_{i}'
                    i += 1

            view_switch = ViewSwitchChoiceMenu(self._default_msg)

            keyboard_view, vert_list = self._get_keyboard()
            type_keyboard = 'inline_keyboard'
            template = dict()
            template[type_keyboard] = keyboard_view
            if self._msg_text is not None:
                template['text'] = self._msg_text
            view_master = ComplexViewMaster(template=template,
                                            view_switch=view_switch,
                                            handlers_dict=self._handlers_dict)

            vertex_list4choice_menu = [dict(vertex_name=vertex_updater_name, control_dict={ITEM_NUM: i})
                                       for i, v in enumerate(vert_list)]

            controller_master = ChoiceInlineMenuController(vertex_list4choice_menu)
            par = view_master.get_param(type_keyboard)
            try:
                controller_master.set_param(dict_name=par['dict_name'])
            except KeyError:
                pass

            # state_keeper = DataItemConnectorCreator(None, 'DataItemForChoiceMenu')
            local_session = DataItemConnectorBuilder(None, None)

            # default_dict=None,
            # in_di_name_list=None, user_input_key=None, env_keys=None
            control_dict_switch = ChoiseMenuDictSwitcher(
                default_dict=None,
                in_di_name_list=None,
                user_input_key=['message', 'callback'],
                env_keys=['_get_me', '_data_id'])

            vertex_blob = VertexBlob(
                local_session_builder=local_session,
                model_keeper=None,
                view_keeper=view_master,
                controller_keeper=controller_master,
                control_dict_switch=control_dict_switch)
            vert_blob_list = list()
            vert_blob_list.append(vertex_blob)
            vertex_builder = VertexBuilder(vert_blob_list=vert_blob_list)
            # vertex_builder.next_vert_def(vertex_name=None)

            vertex_dict[self._vertex_name] = vertex_builder.build(vertex)
            logging.debug('Build choice_menu vertex "{}", text {}'.format(self._vertex_name, self._msg_text))

            # Создаем вершину-обработчик сообщения, которая предназначена для подчистки диалога с пользователем
            # после отработки меню

            if self._update_vertex_blob is not None:
                vertex_blob = self._update_vertex_blob
            else:
                if self._update_msg_text is not None:
                    view_keeper = ComplexViewMaster(template=dict(text=self._update_msg_text),
                                                    handlers_dict=self._handlers_dict)
                else:
                    view_keeper = EMPTY_VIEW_MASTER

                vertex_blob = VertexBlob(local_session_builder=DataItemConnectorBuilder(None, None),
                                         model_keeper=None,
                                         view_keeper=view_keeper,
                                         controller_keeper=DeleterControllerMaster(list()))
            vert_blob_list = list()
            vert_blob_list.append(vertex_blob)

            # temp_l = [dict(id_vert='',
            #                vertex_name=v['vertex_name'],
            #                control_dict=v['control_dict']) for v in vert_list]

            list4vertex_upd = [dict(id_vert=i, vertex_name=v['vertex_name'], control_dict=v['control_dict'])
                               for i, v in enumerate(vert_list)]
            controller_vert_keeper = UpdaterMsgVertexControllerMaster(list4vertex_upd)

            vertex_builder = VertexBuilder(vert_blob_list=vert_blob_list,
                                           controller_vert_keeper=controller_vert_keeper,
                                           wait_signal_bool=False)

            vertex_dict[vertex_updater_name] = vertex_builder.build()

        return vertex_dict


# if __name__ == "__main__":
logging.basicConfig(format=u'  -3- %(levelname)-8s [%(asctime)s] %(message)s', level=logging.DEBUG)
