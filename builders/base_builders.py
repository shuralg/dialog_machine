#!/usr/bin/python3.7
# -*- coding: utf-8 -*-

from dialog_machine.dialog_machine_core import *
from dialog_machine.project_exceptions import *
from dialog_machine.view_masters import ViewSwitch, TextViewHandler, KeyBoardHandler, ComplexViewMaster


class NoneController(ComplexControllerMaster):

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        return dataitem_protect, common_control_dict, msg_control_dict


class VertexBlobBuilder:
    """ Билдер для VertexBlob"""
    _msg_temp = dict(text='Hi! It`s default message.')

    def __init__(self, msg_temp=None, vertex_list=None, data_item=None, parent_data_item=None,
                 model_keeper=None, controller_keeper=None, handlers_dict=None, view_switch=None,
                 control_dict_switch=None, view_keeper=None, state_keeper=None, managed_msg_controller=None):
        if msg_temp is not None:
            self._msg_temp = msg_temp
        self._vertex_list = vertex_list if vertex_list is not None else list()
        self._data_item = data_item
        self._parent_data_item = parent_data_item
        self._controller_keeper = controller_keeper
        self._view_keeper = view_keeper
        self._state_keeper = state_keeper
        self._managed_msg_controller = managed_msg_controller

        if model_keeper is not None:
            self._model_keeper = model_keeper
        else:
            # if not model_connector is None:
            #     self._model_connector = model_connector
            #     init_arg_test = dict(request='Запрос к базе 1')
            #     key_set_test = {'id'}
            #     self._model_keeper = ModelMasterSelect(self._model_connector,
            #                                            init_arg_test,
            #                                            key_set_test)
            # else:
            #     self._model_connector = model_connector
            #     self._model_keeper = model_keeper
            self._model_keeper = None

        self._handlers_dict = handlers_dict if handlers_dict is not None else dict(text=TextViewHandler(),
                                                                                   keyboard=KeyBoardHandler())
        self._view_switch = view_switch if view_switch is not None else ViewSwitch()
        if control_dict_switch is not None:
            self._control_dict_switch = control_dict_switch
        else:
            self._control_dict_switch = DictSwitcherVB()

    def msg_temp(self, value):
        assert isinstance(value, dict)
        self._msg_temp = value
        return self

    def vertex_list(self, value):
        assert type(value) is list
        self._vertex_list = value
        return self

    def data_item(self, value):
        assert type(value) is str or value is None
        self._data_item = value
        return self

    def parent_data_item(self, value):
        assert type(value) is str or value is None
        self._parent_data_item = value
        return self

    def view_switch(self, value):
        assert isinstance(value, ViewSwitch) or value is None
        self._view_switch = value
        return self

    def handlers_dict(self, value):
        assert isinstance(value, dict)
        self._handlers_dict = value
        return self

    def model_keeper(self, value):
        assert isinstance(value, ModelMasterAbstract) or value is None
        self._model_keeper = value
        return self

    def control_dict_switch(self, value):
        assert isinstance(value, DictSwitcherVB) or value is None
        self._control_dict_switch = value
        return self

    def controller_keeper(self, value):
        assert isinstance(value, ComplexControllerMaster) or value is None
        self._controller_keeper = value
        return self

    def state_keeper(self, value):
        assert isinstance(value, DataItemConnectorBuilder)
        self._state_keeper = value
        return self

    def view_keeper(self, value):
        assert isinstance(value, ComplexViewMaster)
        self._view_keeper = value
        return self

    def build(self):
        # собираем vertex_blob
        if self._state_keeper is None:
            state_keeper = DataItemConnectorBuilder(self._data_item, self._parent_data_item)
        else:
            state_keeper = self._state_keeper

        if self._view_keeper is None:
            view_keeper = ComplexViewMaster(deepcopy(self._msg_temp), view_switch=self._view_switch,
                                            handlers_dict=self._handlers_dict, message_default=self._msg_temp)
        else:
            view_keeper = self._view_keeper

        if self._controller_keeper is None:
            controller_keeper = NoneController(self._vertex_list)
        else:
            controller_keeper = self._controller_keeper

        return VertexBlob(local_session_builder=state_keeper, model_keeper=self._model_keeper,
                          view_keeper=view_keeper,
                          controller_keeper=controller_keeper,
                          control_dict_switch=self._control_dict_switch,
                          managed_msg_controller=self._managed_msg_controller)


class VertexBuilder:
    _def_switch = DictSwitcherVB()
    _def_wait_signal_setter = WaitSignalSetter()
    _def_dict_id_getter = DictIdGetter()

    def __init__(self, vert_blob_list=None, dict_id_getter=None,
                 wait_signal_setter=None,
                 wait_signal_bool=True, controller_vert_keeper=None,
                 next_vertex_name_def=None, next_control_dict_def=None):
        self._dict_id_getter = dict_id_getter
        self._wait_signal_setter = wait_signal_setter
        self._controller_vert_keeper = controller_vert_keeper
        self._vert_blob_list = vert_blob_list if vert_blob_list is not None else list()
        self._next_vertex_name_def = next_vertex_name_def
        self._next_control_dict_def = next_control_dict_def
        self._wait_signal_bool = wait_signal_bool

    def vertex_blob(self, value):
        if value is None:
            self._vert_blob_list.clear()
        elif isinstance(value, VertexBlob):
            self._vert_blob_list.append(value)
        return self

    def dict_id_getter(self, value):
        assert isinstance(value, DictIdGetter) or value is None
        self._dict_id_getter = value
        return self

    def wait_signal_setter(self, value):
        assert isinstance(value, WaitSignalSetter) or value is None
        self._wait_signal_setter = value
        return self

    def controller_vert_keeper(self, value):
        assert isinstance(value, ComplexVertexControllerMaster) or value is None
        self._controller_vert_keeper = value
        return self

    def next_vert_def(self, vertex_name, control_dict=None):
        if vertex_name is not None:
            self._next_vertex_name_def = vertex_name
            self._next_control_dict_def = dict() if control_dict is None else control_dict
            self._controller_vert_keeper = None
        return self

    def wait_signal_bool(self, value: bool):
        assert type(value) is bool
        self._wait_signal_bool = value

    def build(self, vertex=None):
        """
        vertex - объект вершины
        return - модифицированный объект вершины """

        if not isinstance(vertex, Vertex) and vertex is not None:
            raise BuilderError('Type Error of vertex')

        # записываем основные составные объекты
        # словари аргуметов
        arg = dict()
        def_arg = dict()  # аргументы по умолчанию

        arg['dict_id_getter'] = self._dict_id_getter
        arg['wait_signal_setter'] = self._wait_signal_setter
        arg['controller_vert_keeper'] = self._controller_vert_keeper
        arg['next_vertex_name_def'] = self._next_vertex_name_def
        arg['next_control_dict_def'] = self._next_control_dict_def
        arg['vert_blob_list'] = self._vert_blob_list
        arg['wait_flag_default'] = self._wait_signal_bool

        def_arg['dict_id_getter'] = self._def_dict_id_getter
        def_arg['wait_signal_setter'] = None
        def_arg['controller_vert_keeper'] = None
        def_arg['next_vertex_name_def'] = None
        def_arg['next_control_dict_def'] = dict()
        def_arg['vert_blob_list'] = list()
        def_arg['wait_flag_default'] = True

        cur_arg = dict()
        if vertex is None:
            # Создаем объект вершины
            for k, v in arg.items():
                cur_arg[k] = v if v is not None else def_arg.get(k, None)
            vertex = Vertex(**cur_arg)
        else:
            for k, v in arg.items():
                if v is not None:
                    cur_arg[k] = v
            vertex.update_vert(**cur_arg)

        return vertex


class BaseBuilder(ABC):
    """ Базовый класс для всех билдеров """

    # Список билдеров на исполнение
    builders_list = list()

    @classmethod
    def builder_list_clear(cls):
        cls.builders_list.clear()

    @classmethod
    def builder_list_append(cls, v):
        assert isinstance(v, cls)
        cls.builders_list.append(v)

    @classmethod
    def builder_list_pop(cls, v_builder):
        """ Метод для удаления билдера из списка в случае вызова его метода build """
        assert isinstance(v_builder, cls)
        # l_i = len(cls.builders_list)-1
        # for i in range(l_i, 0, -1):
        #     v = cls.builders_list[i]
        for i, v in enumerate(cls.builders_list):
            if v is v_builder:
                cls.builders_list.pop(i)
                break

    @classmethod
    def main_build(cls, graph=None):
        vertex_dict = dict() if graph is None else graph
        while True:
            try:
                v = cls.builders_list[0]
            except IndexError:
                break
            else:
                vertex_dict = v.build(vertex_dict)

        return vertex_dict

    def __init__(self):
        self.builder_list_append(self)

    def build(self, vertex_dict):
        """ метод является оболочко для абстрактного метода build """
        self.builder_list_pop(self)
        return self.build_a(vertex_dict)

    @abstractmethod
    def build_a(self, vertex_dict):
        """
                vertex_dict: - словарь "имя: объект вершины" полный аналог графа состояний
                return - возвращает заполненный vertex_dict"""

        return vertex_dict


class SimpleBuilder(BaseBuilder):
    """ Простой Билдер, который просто вставляет вершину в vertex_list """
    default_name = '_simple_vertex_'
    num_def_name = 1

    @classmethod
    def get_name(cls):
        res = cls.num_def_name
        cls.num_def_name += 1
        return cls.default_name + str(res)

    def __init__(self, vertex, vertex_name=None):
        super().__init__()
        self._vertex = vertex
        if vertex_name is not None:
            self.vertex_name = vertex_name
        else:
            self.vertex_name = self.get_name()

    def build_a(self, vertex_dict):
        vertex_dict[self.vertex_name] = self._vertex
        return vertex_dict


class UpdateBuilder(BaseBuilder):
    """ Класс Билдера, который служит для обновления уже имеющейся вершины """

    def __init__(self, vertex_name,
                 vert_blob_list=None, controller_vert_keeper=None,
                 dict_id_getter=None, wait_signal_setter=None, wait_flag_default=None,
                 next_vertex_name_def=None, next_control_dict_def=None):
        super().__init__()
        self.vertex_name = vertex_name
        self.vert_blob_list = vert_blob_list
        self.controller_vert_keeper = controller_vert_keeper
        self.dict_id_getter = dict_id_getter
        self.wait_signal_setter = wait_signal_setter
        self.wait_flag_default = wait_flag_default
        self.next_vertex_name_def = next_vertex_name_def
        self.next_control_dict_def = next_control_dict_def

    def build_a(self, vertex_dict):
        try:
            vertex = vertex_dict[self.vertex_name]
        except KeyError:
            pass
        else:
            vertex.update_vert(
                vert_blob_list=self.vert_blob_list,
                controller_vert_keeper=self.controller_vert_keeper,
                dict_id_getter=self.dict_id_getter,
                wait_signal_setter=self.wait_signal_setter,
                wait_flag_default=self.wait_flag_default,
                next_vertex_name_def=self.next_vertex_name_def,
                next_control_dict_def=self.next_control_dict_def
            )
            vertex_dict[self.vertex_name] = vertex
        return vertex_dict
