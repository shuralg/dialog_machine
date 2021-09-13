#!/usr/bin/python3.7
# -*- coding: utf-8 -*-

""" Реализация билдера для inline-меню """

from dialog_machine.builders.base_builders import *
from dialog_machine.dialog_machine_core import *


class ChoiceControllerMaster(PrototypeControllerMaster):

    def __init__(self, vertex_dict: dict, param_name: str, handler_vertex_name,
                 dict_switcher=None, vert_change_none_name=None):
        """
        :param vertex_dict: {<id1>:dict(vertex_name=..., control_dict=...),..}
        :param dict_switcher: объект класса DictSwitcher для формирования control_dict
        """
        assert isinstance(dict_switcher, ChoiseMenuDictSwitcher) or dict_switcher is None
        super().__init__()
        self._vertex_dict = vertex_dict
        self._param_name = param_name
        self._dict_switcher = dict_switcher
        self._handler_vertex_name = handler_vertex_name
        self._vert_change_none_name = vert_change_none_name

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        if self._dict_switcher is None:
            return dict()
        else:
            arg = dict(user_input=user_input,
                       vert_control_dict=vert_control_dict,
                       env_dict=env_dict,
                       alias_msg=alias_msg)
            return self._dict_switcher.switch(**arg)

    def _prepear_control_dict(self, c_dict, additional_args):
        c_d = dict()
        for key, value in c_dict.items():
            try:
                c_d[key] = additional_args[key]
            except KeyError:
                c_d[key] = value
        return c_d

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        """
        Основной метод выполнения объекта данного класса
        Обеспечивает реализацию основной логики контроллера.
        Возвращает common_control_dict (без привязки к конкретному сообщению)
            и msg_control_dict (словарь команд с привязкой к сообщению) для соответствующего DataItem
        В процессе выполнения модифицирует входящий dataitem_protect
        """
        dataitem_protect[self._param_name] = set(self._vertex_dict.keys())
        if self._dict_switcher is None:
            for k, v in self._vertex_dict.items():
                msg_control_dict[k] = deepcopy(v)
        else:
            for k, v in self._vertex_dict.items():
                v_name = v['vertex_name']
                c_dict = v['control_dict']
                if self._handler_vertex_name == v_name:
                    c_dict['control_dict'] = self._prepear_control_dict(
                        c_dict=c_dict['control_dict'],
                        additional_args=additional_args)
                else:
                    c_dict = self._prepear_control_dict(
                        c_dict=c_dict,
                        additional_args=additional_args)
                msg_control_dict[k] = dict(vertex_name=v_name, control_dict=c_dict)

        if self._vert_change_none_name is not None:
            msg_control_dict[DialogMachineCore.CHANGE_NONE_MSG] = dict(
                vertex_name=self._vert_change_none_name,
                control_dict=dict())

        return dataitem_protect, common_control_dict, msg_control_dict


# Классы для вершины-обработчика сообщения (updater)
class ChoiseMenuDictSwitcher(BaseElement):
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


class ChoiceViewMaster(ViewMaster):
    _vertex_set_def = set()

    def __init__(self, button_template: list, param_name: str, msg_text=None):
        """
        :param button_template: {(<id1>, <button_text1>), (<id2>, <button_text2>), ...}
        :param msg_text: Текст сообщения
        :param param_name: имя параметра в dataitem_protect
        """
        super().__init__()
        self._msg_text = msg_text
        self._button_template = button_template
        self._param_name = param_name

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        # text
        # text = in_view.get('text', None)
        if self._msg_text is not None:
            in_view['text'] = self._msg_text

        # inline_keyboard
        temp_d = dataitem_protect.main_dict
        vertex_set = temp_d.get(self._param_name, self._vertex_set_def)
        inline_keyboard = in_view.get('inline_keyboard', list())
        for v in self._button_template:
            if isinstance(v, list):
                button_line = list()
                for vv in v:
                    try:
                        cur_id = vv[0]
                    except Exception:
                        continue
                    if cur_id not in vertex_set:
                        continue
                    else:
                        button_line.append(dict(text=vv[1], callback_data=cur_id))
                inline_keyboard.append(button_line)
            else:
                try:
                    cur_id = v[0]
                except Exception:
                    continue
                if cur_id not in vertex_set:
                    continue
                else:
                    inline_keyboard.append(dict(text=v[1], callback_data=cur_id))
        in_view['inline_keyboard'] = inline_keyboard
        return in_view


class UpdaterChoiceViewMaster(ViewMaster):

    def main(self, in_view: dict, dataitem_protect, additional_args: dict,
             vertex_context_dict: dict):
        try:
            del in_view['inline_keyboard']
        except KeyError:
            pass
        try:
            del in_view['keyboard']
        except KeyError:
            pass
        return in_view


class UpdaterChoiceControllerMaster(PrototypeControllerMaster):

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
        common_control_dict.clear()
        msg_control_dict.clear()
        return dataitem_protect, common_control_dict, msg_control_dict


class UpdaterChoiceVertexControllerMaster(PrototypeVertexControllerMaster):
    """ Конструктор вершины обработчика сообщения с меню
    (должен удалить сообщение и перейти в нужную вершину) """

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = deepcopy(vert_control_dict)
        return result

    def main(self, additional_args: dict, vertex_context_dict: dict):
        """ Метод реализует основной функционал класса
        """
        control_dict = additional_args.get('control_dict', dict())
        vertex_name = additional_args.get('vertex_name', None)
        return vertex_name, control_dict


class ChoiceMenuBuilderNew(BaseBuilder):
    _param_name = 'inside_data'
    _handler_vertex_name_suf = 'handler_choice_menu'
    _handler_vertex_def_name_suf = 'handler_choice_menu_def'

    def update_vertex_menu_name(self, vertex_menu_name=None):
        if vertex_menu_name is not None:
            self.vertex_menu_name = vertex_menu_name

    def __init__(self, vertex_menu_name, control_list,
                 msg_text=None, dict_switch=None, vert_change_none_name=None):
        """
        :param vertex_menu_name: имя парамера в контексте
        :param vert_change_none_name: имя вершины обработчика изменения сообщения по-умолчанию
        :param control_list: [[(<id1>, <text1>, dict(vertex_name=..., control_dict=...), <vertex_handler1>),
                                (<id2>, <text2>, dict(vertex_name=..., control_dict=...), <vertex_handler2),
                                  ...],
                               (<id3>, <text3>, dict(vertex_name=..., control_dict=...), , <vertex_handler3>)]
                            <vertex_handlerN> - объект класса Vertex, отвечающий за обработку
                                                сообщения до перехода, может быть =None или вовсе отсутствовать
                                                тогда в качестве вершины-обработчика создается дефолтный объект
                             ВНИМАНИЕ: ЕСЛИ <vertex_handlerN> ЗАДАНО, ТО vertex_name НЕ ДОЛЖЕН УКАЗЫВАТЬ
                                       НА УЖЕ СОЗДАННУЮ ВЕРШИНУ, ПОТОМУЧТО В ПРОТИВНОМ СЛУЧАЕ ОНА ЗАТРЕТСЯ
        :param dict_switch: объект класса DictSwitch, определяющий входящие данные
                            для формирования control_dict. Стоит заметить, что в списке
                            control_list тоже фигурирует параметр control_dict, так вот
                            он является control_dict по-умолчанию, и все элементы с ключами
                            имеющимися в нем переносятся на выход, НО ЗНАЧЕНИЯ ПО ЭТИМ КЛЮЧАМ
                            БЕРУТСЯ В ПРИОРИТЕТНОМ ПОРЯДКЕ из словаря, возвращаемого
                            dict_switch
        """
        assert isinstance(dict_switch, ChoiseMenuDictSwitcher) or dict_switch is None
        super().__init__()
        self._control_list = control_list
        self.vertex_menu_name = vertex_menu_name
        self._msg_text = msg_text
        self._dict_switch = dict_switch
        self._vert_change_none_name = vert_change_none_name

    def get_list_item_name(self, i):
        try:
            return self._control_list[i][2]['vertex_name']
        except Exception:
            return None

    class ChoiceViewMasterInside(ChoiceViewMaster):
        pass

    class ChoiceControllerMasterInside(ChoiceControllerMaster):
        pass

    class UpdaterChoiceViewMasterInside(UpdaterChoiceViewMaster):
        pass

    class UpdaterChoiceControllerMasterInside(UpdaterChoiceControllerMaster):
        pass

    class UpdaterChoiceVertexControllerMasterInside(UpdaterChoiceVertexControllerMaster):
        pass

    class DictIdGetterInside(DictIdGetter):
        def main(self, dict_id_in, msg_id_in, additional_args: dict, vertex_context_dict: dict):
            return dict_id_in, msg_id_in

    def _add_vertex(self, vertex_dict, v_list):
        """
        :param vertex_dict:
        :param v_list: (<id1>, <text1>, dict(vertex_name=..., control_dict=...), <vertex_handler1>)
        :return: True - если вершина добавлена, иначе - False
        """
        try:
            vert = v_list[3]
            control_data = v_list[2]
            vertex_name = control_data['vertex_name']
        except Exception as e:
            return False
        else:
            if not isinstance(vert, Vertex):
                return False
            vertex_dict[vertex_name] = vert
            return True

    def build_a(self, vertex_dict):
        handler_vertex_name = '{}_{}'.format(self.vertex_menu_name,
                                             self._handler_vertex_name_suf)

        flag_handler_vertex_name = False

        button_temp = list()
        vertex_dict222 = dict()
        for v in self._control_list:
            if type(v) is list:
                line = list()
                for vv in v:
                    temp_dict = vv[2]
                    if self._add_vertex(vertex_dict, vv):
                        control_data = deepcopy(temp_dict)
                    else:
                        control_data = dict(
                            vertex_name=handler_vertex_name,
                            control_dict=deepcopy(temp_dict)
                        )
                        flag_handler_vertex_name = True

                    line.append((vv[0], vv[1]))
                    vertex_dict222[vv[0]] = control_data
                button_temp.append(line)
            else:
                temp_dict = v[2]
                if self._add_vertex(vertex_dict, v):
                    control_data = deepcopy(temp_dict)
                else:
                    control_data = dict(
                        vertex_name=handler_vertex_name,
                        control_dict=deepcopy(temp_dict)
                    )
                    flag_handler_vertex_name = True
                button_temp.append((v[0], v[1]))
                vertex_dict222[v[0]] = control_data

        menu_vertex_blob = VertexBlob(
            view_keeper=self.ChoiceViewMasterInside(
                button_template=button_temp,
                param_name=self._param_name,
                msg_text=self._msg_text),
            controller_keeper=self.ChoiceControllerMasterInside(
                vertex_dict=vertex_dict222,
                param_name=self._param_name,
                handler_vertex_name=handler_vertex_name,
                dict_switcher=self._dict_switch,
                vert_change_none_name=self._vert_change_none_name
            )
        )
        try:
            vertex_menu = vertex_dict[self.vertex_menu_name]
        except KeyError:
            # Создаем новую вершину
            vertex_menu = Vertex(
                vert_blob_list=[menu_vertex_blob],
                dict_id_getter=self.DictIdGetterInside(),
                wait_flag_default=True,
                next_vertex_name_def=None,
                next_control_dict_def=dict())
            vertex_dict[self.vertex_menu_name] = vertex_menu
        else:
            vertex_menu.update_vert(vert_blob_list=[menu_vertex_blob])
            vertex_dict[self.vertex_menu_name] = vertex_menu

        # Создаем вершину-обработчик
        if flag_handler_vertex_name:
            updater_vertex = Vertex(
                vert_blob_list=[VertexBlob(
                    view_keeper=self.UpdaterChoiceViewMasterInside(),
                    controller_keeper=self.UpdaterChoiceControllerMasterInside())],
                dict_id_getter=self.DictIdGetterInside(),
                controller_vert_keeper=self.UpdaterChoiceVertexControllerMasterInside(),
                wait_flag_default=False)
            vertex_dict[handler_vertex_name] = updater_vertex
        return vertex_dict


class ChoiceMenuSimpleBuilder(ChoiceMenuBuilderNew):
    """ Класс, который просто перенаправляет
        результат работы dict_switcher на выход и добавляет к этому результату то,
        что было указано
        в значении по-умолчанию (2 параметра элемента control_list))"""

    class ChoiceControllerMasterInside(ChoiceControllerMaster):
        class DictSwitcher(ChoiseMenuDictSwitcher):
            def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
                return deepcopy(vert_control_dict)

        def __init__(self, vertex_dict: dict, param_name: str, handler_vertex_name,
                     dict_switcher=None, vert_change_none_name=None):
            if dict_switcher is None:
                dict_switcher = self.DictSwitcher()
            super().__init__(vertex_dict, param_name, handler_vertex_name, dict_switcher, vert_change_none_name)

        def _prepear_control_dict(self, c_dict, additional_args):
            res = deepcopy(additional_args)
            for key, value in c_dict.items():
                res[key] = value
            return res
