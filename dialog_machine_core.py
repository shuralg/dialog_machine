#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

import logging
from copy import deepcopy

# import psycopg2
from contracts import contract

from dialog_machine.interfaces import ContextStorage, MsgGetter, SenderMsg
from dialog_machine.project_exceptions import *

from abc import ABC, abstractmethod

from dialog_machine.session_master import DataItemConnectorBuilder, DataItemConnector, \
    SessionManagerCollection

from uuid import uuid4

from pprint import pformat
import hashlib


class StateMachine:
    """ Класс конечного автомата (хранит граф состояний, начальную вершину этого графа,
            а также начальное состояние переменных контекста)"""

    # TODO: С учетом того, что начальный элемент фактически задается в MsgGetter, то скорректировать
    #       данный класс, например переименовать "begin_vertex" в "default_vertex"

    # @contract(param='dict_param_state_machine_contract')
    def __init__(self, graph, begin_vertex_name, begin_var_state=None):
        self._begin_vertex_name = begin_vertex_name
        self._graph = graph
        self._begin_var_state = begin_var_state if isinstance(begin_var_state, dict) else dict()
        self._begin_vertex = self._graph[self._begin_vertex_name]
        # except KeyError as e:
        #     logging.error('Non exist element in graph. Exception in StateMachine init. Exception: %s' % str(e))
        #     raise StateMachineError('Non exist element in graph')

    # @contract(returns='is_vertex')
    def get_begin_vertex(self):
        """ Получить начальную вершину графа состояний"""
        return self._begin_vertex

    def get_begin_vertex_name(self):
        """ Получить имя начальной вершины графа состояний"""
        if self._begin_vertex_name is not None:
            return self._begin_vertex_name
        else:
            raise StateMachineError('Is not begin vertex name')

    @contract(returns='dict')
    def get_begin_var_state(self):
        """ Получить начальное состояние переменных контекста """
        return self._begin_var_state

    # @contract(name='str', returns='is_vertex|None')
    def get_vertex_by_name(self, name):
        """ Получить вершину по имени"""
        try:
            return self._graph[name]
        except KeyError:
            return self._graph[self.get_begin_vertex_name()]

    # @contract(vertex='is_vertex', returns='str|None')
    def get_name_by_vertex(self, vertex):
        """ Получить имя по вершине"""
        for k, v in self._graph.items():
            if v is vertex:
                return k
        return None

    # @contract(vertex_checker='$VertexChecker', returns='is_vertex|None')
    # def find_vertex(self, vertex_checker):
    #     """ Ищем вершину по каком-то признаку, переданному в объекте vertex_checker
    #      vertex_checker- на вход получает вершину графа состояний, а выдает True или False"""
    #     for k, v in self._graph.items():  # type: (str, Vertex)
    #         try:
    #             if vertex_checker(v):
    #                 return v
    #         except Exception as e:
    #             logging.warning("Something wrong in vertex_checker: {}".format(str(e)))
    #     return None


class ContextConnector:
    """ Класс объектов работы с контекстом
        предоставляет интерфейс:
         1) получения текущего контекста (словаря переменных состояния (var_state),
                                            имени самого состояния (vertex_name),
                                            словаря управляющих данных для этого состояния (control_dict),
                                            объекта сообщения (msg_obj),
                                            идентификатора сообщения (msg_id)
                                          )
         2) записи инфомации, необходимой для отправки сообщения (dict_id, msg_id, msg_obj,
                                                                    common_dict, msg_dict)
         3) запись текущего контекста (все из п.1)

    """
    _msg_context_obj = dict()
    _sending_msg = True  # флаг, показывает нужно ли помещать текущее сообщение в граф отправки
    _main_dict_id = dict()
    _is_first_msg = False  # Первое сообщение серии
    _cur_msg_is_first = False  # Флаг показывает, что текущее сообщение является первым в серии

    _cur_msg = None  # Текущее Сообщение, для которого будут вставлены данные по-умолчанию
    _none_control_data = None  # имя вершины и управляющие данные для сообщения по умолчанию для первого элемента серии
    _control_data = None  # имя вершины и управляющие данные для сообщения по умолчанию

    # _none_control_data=None или = dict(vertex_name=..., control_dict=...)

    def __init__(self, msg_sender, msg_getter, msg_obj_default, start_control_param):
        """
            msg_sender - объект для отправки сообщений
            msg_getter - объект хранитель информации о сообщениях (какой вершине графа состояний
                    соответствует сообщение, какой control_dict ему передавайть)
            msg_obj_default - объект сообщения по умолчанию
            start_control_param - стартовые параметры (стартовые vertex_name и control_dict)
        """
        assert isinstance(msg_sender, SenderMsg)
        assert isinstance(msg_getter, MsgGetter)
        assert isinstance(start_control_param, dict)

        self._msg_sender = msg_sender
        self._msg_sender.set_msg_getter(msg_getter)  # убрать ??
        self._msg_getter = msg_getter
        self._start_control_param = start_control_param

        # self._is_sent = False  # флаг, показывающий отправлен ли был контекст (нужно для деструктора)

        # self._msg_id = msg_id
        # self._dict_id = dict_id
        # self._command = command

        self._msg_obj_default = msg_obj_default if msg_obj_default is not None else dict()

        # self._context_obj - главный контекст диалога (текущий контект)
        # self._msg_context_obj - контекст формируемого сообщения
        # self._context_obj = None  # экземпляр контекста для dict_id:({vertex_name, control_dict})
        # self._msg_context_obj = None  # словарь в который собираем текущий контекст для отправки сообщения
        # self._cur_context_obj = None  # текущие значения vertex_name и control_dict

        self._sending_msg = True  # флаг, показывает нужно ли помещать текущее сообщение в граф отправки
        self._msg_list = list()  # список данных необходимых для отправки сообщений
        #           (экземпляры self._msg_context_obj)
        # self._get_context()
        # self._main_dict_id = main_dict_id
        #
        # self._msg_getter.acquire_main_lock(dict_id=main_dict_id)
        #
        # # if not (isinstance(self._msg_context_obj, dict) and dict_id == self._msg_context_obj['dict_id']):
        # t = self._context_store.get_context(self._main_dict_id)
        # self._var_state = t.get('var_state', self._start_control_param.get('var_state', dict()))
        self._main_dict_id = None
        self._alias_def_msg = None  # Алиас сообщения по-умолчанию в текущем контексте
        self._handler_alias_msg_set = set()  # множество алиасов обработанных сообщений
        self._num_in_msg_list = None  # номер сообщения в self._msg_list, которое мы повторно достали и меняем

    def _get_msg_context_def(self):
        """ Выдает контекст нового сообщения """
        alias_msg = str(uuid4())
        return dict(dict_id=deepcopy(self._main_dict_id),
                    msg_id=None,
                    msg_obj=deepcopy(self._msg_obj_default),
                    common_dict=dict(),
                    msg_dict=dict(),
                    is_msg_changed=True,
                    is_msg_dict=False,
                    is_new_msg=True,  # Флаг говорит о том, что сообщение новое
                    alias_msg=alias_msg
                    )

    def init_main_context(self, dict_id: dict, msg_id=None, command=None, alias_msg=None):
        """ Инициализация контекста основоного сообщения """
        if dict_id is None:
            if alias_msg is not None:
                dict_id = self._msg_getter.get_dict_id_by_alias(alias_msg=alias_msg)
            else:
                raise ContextConnectorError("Param error: dict_id and alias_msg are None")

        # Устанавливаем главный лок и получаем контекст
        # Установить главный лок и self._main_dict_id можно только один раз
        if self._main_dict_id is None:
            self._main_dict_id = dict_id
            self._alias_def_msg = self._msg_getter.acquire_main_lock(dict_id=self._main_dict_id)

        if alias_msg is not None:
            # dict_id, msg_id = self._msg_getter.get_dict_id_by_alias(alias_msg=alias_msg)
            control_data, self._msg_context_obj = self._msg_getter.get_msg_by_alias(
                alias_msg=alias_msg,
                dict_id=dict_id,
                command=command)
        else:
            control_data, self._msg_context_obj = self._msg_getter.get_msg(
                dict_id=dict_id,
                msg_id=msg_id,
                command=command)

        if control_data is not None:
            vertex_name = control_data.get('vertex_name', None)
            control_dict = control_data.get('control_dict', dict())
        else:
            # vertex_name = self._start_control_param.get('vertex_name', None)
            # control_dict = self._start_control_param.get('control_dict', dict())
            raise NoMatchCommandError("Command '{}' is empty in msg_dict".format(command))
        self._get_context()

        # Если запрашиваемого сообщения нет (msg_context_obj==None),
        # то выполнение вершины графа состояний, которая обрабатывает это несуществующее сообщение, не нужно,
        # однако от этой вершины нам нужно получить только имя следующей вершины и управляющие данные для нее,
        # поэтому создаем фиктивное сообщение, которое будет передаваться вершине графа состояний,
        # но отправляться не будет, за этим будет следить соответствующий флаг
        if self._msg_context_obj is None:
            self._msg_context_obj = self._get_msg_context_def()
            self._sending_msg = False

        return vertex_name, control_dict

    def get_msg_from_list(self, alias_msg, command=None):
        """ Ищет сообщение и управляющие данные в self._msg_list """
        control_data_ = None
        msg_context_obj = None
        # self._num_in_msg_list = None
        for i, v in enumerate(self._msg_list):
            if v.get('alias_msg', None) == alias_msg:
                msg_context_obj = deepcopy(v)
                self._num_in_msg_list = i
                if command is not None:
                    msg_dict = v.get('msg_dict', dict())
                    dict_id = v.get('dict_id', dict())
                    try:
                        temp_dict = msg_dict[command]
                    except KeyError:
                        try:
                            temp_dict = self._msg_getter.get_control_msg_(dict_id, command)
                        except KeyError:
                            raise NoMatchCommandError("Command '{}' is empty in msg_dict".format(command))
                    t = deepcopy(temp_dict)
                    control_data_ = dict(vertex_name=t.get('vertex_name', None),
                                         control_dict=t.get('control_dict', dict()))
                break
        return control_data_, msg_context_obj

    def init_controlled_context(self, alias_msg, command=None):
        """ Инициализация контекста управляемого сообщения """
        # Сначала ищем dict_id и сообщение в self._msg_list, только если не находим,
        # то обращаемся к self._msg_getter
        control_data, msg_context_obj_ = self.get_msg_from_list(alias_msg=alias_msg,
                                                                command=command)
        if msg_context_obj_ is not None:
            self._msg_context_obj = msg_context_obj_
        else:
            dict_id = self._msg_getter.get_dict_id_by_alias(alias_msg=alias_msg)
            control_data, self._msg_context_obj = self._msg_getter.get_msg_by_alias(alias_msg=alias_msg,
                                                                                    dict_id=dict_id,
                                                                                    command=command)
        self._get_context()

        if self._msg_context_obj is None:
            raise NoSuchMsgError("No such msg error by alias = '{}'".format(alias_msg))

        if control_data is not None:
            vertex_name = control_data.get('vertex_name', None)
            control_dict = control_data.get('control_dict', dict())
        else:
            vertex_name = None  # self._start_control_param.get('vertex_name', None)
            control_dict = None  # self._start_control_param.get('control_dict', dict())
            # raise NoMatchCommandError("Command is empty in msg_dict")

        return vertex_name, control_dict

    def _get_context(self):
        """ контекст текущего сообщения (_msg_context_obj ) + var_state """

        # Находим сообщение по умолчанию ( с ключом None и команды к нему), если оно есть.
        # Это необходимо, чтоб потом перезаписать

        if self._msg_context_obj is not None:
            common_dict = self._msg_context_obj.get('common_dict', dict())
            try:
                self._none_control_data = common_dict[None]
            except KeyError:
                self._none_control_data = None
            else:
                del common_dict[None]
                self._msg_context_obj['common_dict'] = common_dict
        else:
            self._none_control_data = None

        # self._is_sent = False
        self._is_first_msg = True
        self._cur_msg_is_first = False  # Флаг показывает, что текущее сообщение является первым в серии

        self._cur_msg = None  # Текущее Сообщение, для которого будут вставлены данные по-умолчанию
        self._control_data = None  # имя вершины и управляющие данные для сообщения по умолчанию

    @property
    def main_dict_id(self):
        return self._main_dict_id

    # Методы и свойства для контекста сообщения
    @property
    def msg_dict_id(self):
        try:
            return self._msg_context_obj['dict_id']
        except:
            return None

    @msg_dict_id.setter
    def msg_dict_id(self, value):
        if value is None:
            return
        if self._msg_context_obj is not None:
            if self._msg_context_obj.get('is_new_msg', False) and self._msg_context_obj['msg_id'] is None:
                # value != self._main_dict_id:
                self._msg_context_obj['dict_id'] = deepcopy(value)

    @property
    def msg_alias(self):
        """ Свойство получения алиаса сообщения (идентификатора сообщения,
        который известен уже до отправки самого сообщения). Менять его нельзя!!!"""
        try:
            return self._msg_context_obj['alias_msg']
        except:
            return None

    @property
    def msg_msg_id(self):
        try:
            return self._msg_context_obj['msg_id']
        except:
            return None

    @msg_msg_id.setter
    def msg_msg_id(self, value):
        if self._msg_context_obj is not None:
            self._msg_context_obj['msg_id'] = deepcopy(value)

    def full_msg_id(self, id_dict):
        if id_dict is not None and self._msg_context_obj is not None:
            try:
                self._msg_context_obj['msg_id'] = id_dict['msg_id']
            except KeyError:
                pass
            try:
                self._msg_context_obj['dict_id'] = id_dict['dict_id']
            except KeyError:
                pass

    @property
    def msg_msg_obj(self):
        try:
            return deepcopy(self._msg_context_obj['msg_obj'])
        except:
            return None

    @msg_msg_obj.setter
    def msg_msg_obj(self, value):
        # TODO: При msg_obj == None скорее всего подразумевается, что сообщение не отправляется,
        #       но обработка производится. Это не окончательное решение, возможно реализовать такой эффект,
        #       когда msg_obj['text'] == None или == "". Продумать!!!
        # if value is None:
        #     value = self._msg_obj_default
        # if self._msg_context_obj is not None:
        if self._msg_context_obj['msg_obj'] != value:
            self._msg_context_obj['msg_obj'] = value
            self._msg_context_obj['is_msg_changed'] = True

    @property
    def msg_common_dict(self):
        try:
            return deepcopy(self._msg_context_obj['common_dict'])
        except:
            return None

    @msg_common_dict.setter
    def msg_common_dict(self, value):
        if value is None or self._msg_context_obj is None:
            return
        if self._msg_context_obj['common_dict'] != value:
            self._msg_context_obj['common_dict'] = value
            # is_msg_changed = False, is_msg_dict = False
            self._msg_context_obj['is_msg_dict'] = True

    @property
    def msg_msg_dict(self):
        try:
            return deepcopy(self._msg_context_obj['msg_dict'])
        except:
            return None

    @msg_msg_dict.setter
    def msg_msg_dict(self, value):
        if value is None or self._msg_context_obj is None:
            return
        if self._msg_context_obj['msg_dict'] != value:
            self._msg_context_obj['msg_dict'] = value
            self._msg_context_obj['is_msg_dict'] = True

    @property
    def get_changed_def_alias_msg(self):
        """ Возвращает алиас бывшего сообщения по-умолчанию если оно потеряло этот свой статус
            (это свойство необходимо для обработки сообщения, которое потеряло свой статус
            и при этом не было отработано) """
        if self._cur_msg is not None \
                and not self._cur_msg_is_first \
                and self._alias_def_msg != self._cur_msg['alias_msg'] \
                and (self._alias_def_msg not in self._handler_alias_msg_set):
            return self._alias_def_msg
        else:
            return None

    def set_msg(self, msg_data):
        try:
            dict_id = msg_data['dict_id']
            msg_obj = msg_data['msg_obj']
            common_dict = msg_data['common_dict']
            msg_dict = msg_data['msg_dict']
        except KeyError:
            return
        else:
            self.msg_msg_obj = msg_obj
            self.msg_common_dict = common_dict
            self.msg_msg_dict = msg_dict
            self.msg_dict_id = dict_id

    def _append_to_msg_list(self, msg_context_obj):
        """ Умное добавление отправляемого сообщения к списку отправки
        (склеивает сообщения с одинаковыми алиасами)"""
        if self._num_in_msg_list is not None:
            self._msg_list[self._num_in_msg_list] = msg_context_obj
            self._num_in_msg_list = None
        else:
            self._msg_list.append(msg_context_obj)

    def simple_msg_send(self, vertex_name=None, control_dict=None, to_set_none_el=True):
        """ Метод просто заносит текущее сообщение в смисок для отправки
        to_set_none_el - флаг показывает нужно ли возвращать элемент None
        (для обработки изменения элемента по-умолчанию это не нужно,
        а в остальных случаях необходимо)
         """
        if self._sending_msg:
            if to_set_none_el:
                if self._main_dict_id == self.msg_dict_id:
                    self._cur_msg = self._msg_context_obj
                    self._cur_msg_is_first = self._is_first_msg
                else:
                    # Если первое сообщение вне главного контекста, то необходимо
                    # вернуть его статус сообщения по-умолчанию
                    if self._is_first_msg and self._none_control_data is not None:
                        common_dict = self.msg_common_dict
                        common_dict[None] = self._none_control_data
                        self.msg_common_dict = common_dict

            if vertex_name is None:
                self._control_data = None
            else:
                self._control_data = dict(vertex_name=vertex_name, control_dict=control_dict)

            # if not (self._msg_context_obj.get('is_new_msg', False) and self.msg_dict_id is None):
            if self.msg_dict_id is not None:
                # Если self.msg_dict_id is None, значит неизветсно куда отправлять сообщение,
                # следовательно оно не должно быть отправлено
                self._handler_alias_msg_set.add(self.msg_alias)
                self._append_to_msg_list(self._msg_context_obj)
        else:
            self._sending_msg = True

        self._is_first_msg = False

    def msg_send(self, vertex_name, control_dict: dict):
        """ Отсылаем данные сообщения на отправку и обнуляем словарь self._msg_context_obj
            vertex_name, control_dict - вершина  и управляющие данные для обработки
                    данного сообщения. которое становится сообщением по-умолчанию
        """
        # изменяем текущее состояние в зависимостиот того корректировали ли ранее
        # отправленное сообщение или создавали новое

        logging.debug("!*! msg_send self.msg_msg_id={}".format(self.msg_msg_id))

        self.simple_msg_send(vertex_name=vertex_name,
                             control_dict=control_dict,
                             to_set_none_el=True)

        self._msg_context_obj = self._get_msg_context_def()

    def set_context(self):
        # перед отправкой добавляем элемент по-умолчанию в нужное сообщение
        if self._cur_msg is not None:
            common_dict = self._cur_msg.get('common_dict', dict())
            if self._control_data is None:
                if self._cur_msg_is_first:
                    if self._none_control_data is not None:
                        common_dict[None] = self._none_control_data
                else:
                    common_dict[None] = dict(vertex_name=self._start_control_param.get('vertex_name', None),
                                             control_dict=self._start_control_param.get('control_dict', dict()))
            else:
                common_dict[None] = self._control_data

            self._cur_msg['common_dict'] = common_dict

        # отправляем накопленные сообщение, в котором is_new_msg==False
        # (оно должно быть одно, но на всякий случай проходим весь список)
        temp_list = list()
        for i, m in enumerate(self._msg_list):
            if not m.get('is_new_msg', False):
                temp_list.append(i)
                logging.debug('ContextConnector set_context : {}'.format(m))
                self._msg_sender(m)
        temp_list.reverse()
        for i in temp_list:
            self._msg_list.pop(i)

        # t = dict(var_state=self._var_state)
        # self._context_store.set_context(self._main_dict_id, t)
        # self._is_sent = True

    # def clear_context_store(self):
    #     """ Разблокирует заблоченый context_store """
    #     try:
    #         self._context_store.set_context(self._main_dict_id, None)
    #     except IsContextProcessError:
    #         pass

    def send_msg_list(self):
        """ Отправляет список неотправленных сообщений из self._msg_list"""
        # отправляем накопленные сообщения
        for m in self._msg_list:
            logging.debug('ContextConnector set_context : {}'.format(m))
            self._msg_sender(m)
        self._msg_list.clear()

    def __del__(self):
        # if not self._is_sent:
        # разблокируем хранителя контекста при удалении текущего объекта
        if self._main_dict_id is not None:
            self._msg_getter.release_main_lock(self._main_dict_id)
        # super().__del__()


class AbstractFilterDictId(ABC):
    """Класс фильтр, для проверки подходит ли сообщение с конкретным dict_id для каких-то действий или нет
     __call__ возвращает True или False"""

    @abstractmethod
    def __call__(self, dict_id):
        return True


class MsgGetterOld(ABC):
    """ Класс объекта хранилища данных сообщений и контекста этих сообщений. Прототип """

    # TODO: Реализовать обработку alias_msg во всех потомках MsgGetter

    # TODO: обработать вариант, когда сообщения не с заданным id или алиасом нет (внести соответствующее сообщение)

    def __init__(self, common_dict_def, *args, **kwargs):
        """ Если какая-то команда отсутствует, она может быть взята из common_dict_def
            self._common_command_dict - словарь команд по умолчанию
                формат:
                    {'command1': [{filter: <filter_obj1>, vertex_name: <name1>, control_dict: <cont_dict1>},
                                  {filter: <filter_obj2>, vertex_name: <name2>, control_dict: <cont_dict2>},
                                  ...
                                ]
                     'command2': [{filter: <filter_obj3>, vertex_name: <name3>, control_dict: <cont_dict3>},
                                  {filter: <filter_obj4>, vertex_name: <name4>, control_dict: <cont_dict4>},
                                  ...
                                ]
                    ...
                    }
        """
        assert isinstance(common_dict_def, dict)
        self._common_dict_def = common_dict_def
        self._common_command_dict = dict()

    def add_filter(self, filter_getter, command, vertex_name, control_dict):
        # TODO: Данная реализация несколько отходит от принципа единственности ответственности для объекта
        #       Реализовано таким образом. чтоб не плодить еще один дополнительный класс,
        #       коих и без того много. Рекомендуется вынести функционал запроса vertex_name и control_dict по умолчанию
        #       в отдельный объект, который будет задаваться в конструкторе MsgGetter
        """
        Метод для добавления команды по умолчанию
        :param filter_getter: AbstractFilterDictId
        :param command: str
        :param vertex_name: str
        :param control_dict: dict
        """
        temp_list = self._common_command_dict.get(command, list())
        temp_list.append(dict(filter=filter_getter, vertex_name=vertex_name, control_dict=control_dict))
        self._common_command_dict[command] = temp_list

    def _get_control(self, dict_id, command):
        """ Внутренний метод для получения значений vertex_name и control_dict по умолчанию,
            если соответствующих значений не находит. то выбрасывает KeyError"""
        try:
            temp_list = self._common_command_dict[command]
        except KeyError:
            try:
                return self._common_dict_def[command]
            except KeyError:
                if command is None:
                    raise NoneCommandError()
                else:
                    raise NoMatchCommandError()
        else:
            for v in temp_list:
                filter_getter = v['filter']
                if filter_getter(dict_id):
                    return dict(vertex_name=v['vertex_name'], control_dict=v['control_dict'])
            try:
                return self._common_dict_def[command]
            except KeyError:
                if command is None:
                    raise NoneCommandError()
                else:
                    raise NoMatchCommandError()

    @abstractmethod
    def inc_semaphore(self, dict_id):
        """ Увеличивает семафор, метод необходим из-за того
            что get_msg вызывается только один раз за сеанс, при этом во время сеанса
            может быть отправлено произвольтное количество сообщений (а соответсвенно вызван метод set_msg),
            чтоб скорректировать вызовы и сделать их симметричными, вводится этот метод"""
        pass

    @abstractmethod
    def dec_semaphore(self, dict_id):
        """ Уменьшаем семафор """
        pass

    @abstractmethod
    def get_msg(self, dict_id, msg_id, command):
        return dict(vertex_name=None, control_dict=dict()), None
        # dict(dict_id=dict_id, msg_id=msg_id, msg_obj=None, common_dict=dict(), msg_dict=dict(),
        #      is_msg_changed=False, is_msg_dict=False)

    @abstractmethod
    def get_id_by_alias(self, alias_msg):
        """ возвращает идентификатор сообщения в системе по алиасу"""
        dict_id = dict()
        msg_id = None
        return dict_id, msg_id

    @abstractmethod
    def get_msg_by_alias(self, alias_msg):
        """ возвращает сообщение (все данные сообщения),
            вершину графа состояний и управляющие данные по алиасу """
        return None
        # dict(dict_id=dict_id, msg_id=msg_id, msg_obj=None, common_dict=dict(), msg_dict=dict(),
        #      is_msg_changed=False, is_msg_dict=False)

    @abstractmethod
    def set_msg(self, dict_msg_arg):
        """ Устанавливает данные сообщения.
            dict_msg_arg - cодержит все данные сообщения в томи числе dict_id  и msg_id"""
        pass


class AbstractInteractionClass(ABC):
    """ Класс содержащий в себе методы взаимодействия с ботом
        абстракция-заглушка для работы конечных автоматов в текущем модуле
        должен быть переопределеен в основном тексте программы исходя из
        нюансов API для бота """

    @abstractmethod
    def send_text(self, dict_id, text, markdown, markdown_inline, flag_to_return=True):
        """ flag_to_return - флаг, показывающий нужно ли возвращать id сообщения (в составе dict_id)
        :type text: object
        """
        if flag_to_return:
            return dict_id

    @abstractmethod
    def send_photo(self, dict_id, text, photo_list, markdown, markdown_inline, flag_to_return=True):
        if flag_to_return:
            return dict_id

    @abstractmethod
    def send_video(self, dict_id, text, video_list, markdown, markdown_inline, flag_to_return=True):
        if flag_to_return:
            return dict_id

    @abstractmethod
    def send_audio(self, dict_id, text, audio_list, markdown, markdown_inline, flag_to_return=True):
        if flag_to_return:
            return dict_id

    @abstractmethod
    def send_file(self, dict_id, text, file_list, markdown, markdown_inline, flag_to_return=True):
        if flag_to_return:
            return dict_id

    @abstractmethod
    def update_text(self, dict_id, text, markdown, markdown_inline):
        return True

    @abstractmethod
    def update_photo(self, dict_id, text, photo_list, markdown, markdown_inline):
        return True

    @abstractmethod
    def update_video(self, dict_id, text, video_list, markdown, markdown_inline):
        return True

    @abstractmethod
    def update_audio(self, dict_id, text, audio_list, markdown, markdown_inline):
        return True

    @abstractmethod
    def update_file(self, dict_id, text, file_list, markdown, markdown_inline):
        return True

    @abstractmethod
    def del_msg(self, dict_id):
        return True


class BaseElement(ABC):
    """ Родитель для всех составляющих элементов Vertex и VertexBlob,
        реализует функционал мультиплексора данных"""

    def __init__(self, default_dict=None,
                 in_di_name_list=None, user_input_key=None, env_keys=None, alias_msg_name='alias_msg',
                 **kwargs):
        if in_di_name_list is None:
            in_di_name_list = list()
        else:
            assert isinstance(in_di_name_list, list) and sum(
                (1 for v in in_di_name_list if not isinstance(v, str))) == 0
        assert type(user_input_key) is list or user_input_key is None
        assert type(env_keys) is list or env_keys is None
        assert type(default_dict) is dict or default_dict is None
        assert type(alias_msg_name) is str

        self._in_di_name_list = in_di_name_list
        self._user_input_key = user_input_key if user_input_key is not None else list()
        self._env_keys = env_keys if env_keys is not None else list()
        self._default_dict = default_dict if default_dict is not None else dict()
        self._alias_msg_name = alias_msg_name

    # def __call__(self, **args):
    #     additional_args = self.switch(**args)
    #     return self.main(additional_args=additional_args, **args)

    @abstractmethod
    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        """ Коммутирует данные из разных источников в один словарь, который передается в additional_args
        alias_msg - Алиас сообщения, идентификатор не зависящий от способа адресации сообщений
                    на конкретной платформе, необходим для управления
        """
        return dict()

    # @abstractmethod
    # def main(self, additional_args: dict, **args):
    #     """ Отрабатывает основную логику элемента """
    #     return None


class DictSwitcherVB(BaseElement):
    """ Записывает _dict в data_item перед выполнением модели
        На вход принимает помимо данных формируемых switcher еще и текущий data_item
        поэтому метод становится внутренним для класса VertexBlob отсюда и 'VB' в названии"""

    def __init__(self, default_dict=None,
                 in_di_name_list=None, user_input_key=None, env_keys=None):
        """
            default_dict - словарь по умолчанию, который участвует в формировании результирующего словаря
            in_di_name_list - список DataItem данные которых будут участвовать в формировании результирующего словаря
            user_input_key - список ключевых полей из user_input для формирования результирующего словаря
            env_keys - список ключевых полей из env_dict для формирования результирующего словаря
        """
        super().__init__(default_dict=default_dict,
                         in_di_name_list=in_di_name_list, user_input_key=user_input_key, env_keys=env_keys)

    # def __call__(self, data_item, **args):
    #     args['data_item'] = data_item
    #     return super().__call__(**args)

    def main(self, data_item, additional_args: dict, vertex_context_dict: dict):
        """ Отрабатывает основную логику элемента
        """
        return additional_args

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        """ Коммутирует данные из разных источников в один словарь, который передается в additional_args
        """
        return dict()


#
# Классы работы с моделью
#

class MediaMasterAbstract(ABC):
    """ Класс объекта, определяющего физический доступ
            к медиа данным по file_id (к физическим картинкам, видео, аудио ...) """

    @abstractmethod
    def get_file_id(self, file):
        """ Возвращает id по файлу """
        return None

    @abstractmethod
    def get_file(self, file_id, filter_type=None):
        """ Возвращает файл по id  """
        return None


class ModelConnectorBase:
    """ Базовый класс соединения к хранилищу модели (это не обязательно база данных - это может быть
        какой-нибудь объект c доступом через API)  """

    _conn_id = [0]

    def __init__(self):
        self._id = self._conn_id[0]
        self._conn_id[0] = self._id + 1

    def create_connection(self):
        """ Создает соединение с базой данных и возвращает его """
        phisical_connection = None
        return phisical_connection

    def get_id(self):
        """ Возвращает уникальный идентификатор конеретного объекта соединения
            (неизменяемый тип, который можно использовать в качестве ключа элемента словаря)"""
        return self._id

    def close_connection(self, conn):
        """ Закрываем соединение соединение с базой данных и возвращает его
        :param conn:
        """
        pass

    def commit(self, conn):
        """ коммитим запрос в базу
        :param conn:
        """
        pass

    def execute(self, conn, master_arg, arg):
        """ Метод-заглушка зарезервированный для вариантов реализации, для котороых необходимо
            заключить весь функционал работы с моделью объект ModelConnector
            conn - соединение
            master_arg - аргументы от мастера модели, которые задаются
                        на этапе реализации или инициализации (например текст запроса)
            arg - аргументы этапа выполнения"""
        return

    def rollback(self, conn):
        """ Откатывает транзакцию в случае возникновения ошибок
            conn - объект соединения с моделью, созданный в методе create_connection
                        и переданный обратно в внешним окружением"""
        return


class ModelConnection:
    """ Класс объекта соединения, объект этапа выполнения, адаптер для объекта-потомка ModelConnectorInterface """

    def __init__(self, model_conn_id, model_connector=None):
        assert isinstance(model_connector, ModelConnectorBase) or model_connector is None
        self._model_connector = model_connector
        self._ph_conn = model_connector.create_connection()
        self._model_conn_id = model_conn_id

    def commit(self):
        self._model_connector.commit(self._ph_conn)

    def close_connection(self):
        self._model_connector.close_connection(self._ph_conn)
        self._model_connector = None

    def __del__(self):
        if self._model_connector is not None:
            self.close_connection()

    def get_id(self):
        return self._model_connector.get_id()

    def get_conn(self):
        return self._ph_conn

    def rollback(self):
        return self._model_connector.rollback(self._ph_conn)


class ModelMasterAbstract(BaseElement):
    """ Класс доступа к модели
    """
    _model_connector = ModelConnectorBase()

    def __init__(self, **args):
        """
            Предполагается использовать исключения PhysicalModelError и LogicalModelError.
            LogicalModelError - особенно важно, так как на это исключение завязана обработка
             в классе Vertex, оно должно выбрасываться в случае логических проблем в запросе модели
             (например данные не могут быть вставлены или обновлены по какой-то причине и т.д.)
        """
        super().__init__(**args)

        self._key_set = None

    def get_model_connector(self):
        return self._model_connector

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        """ Коммутирует данные из разных источников в один словарь, который передается в additional_args
        """
        result = dict()

        if vert_control_dict is not None:
            for k, v in vert_control_dict.items():
                result[k] = deepcopy(v)

        if user_input is not None:
            for k in self._user_input_key:
                try:
                    result[k] = deepcopy(user_input[k])
                except KeyError:
                    pass

        return result

    def get_key(self):
        return self._key_set

    @property
    def key_set(self):
        return self._key_set

    # def get_arg(self, cur_data_item, additional_args):
    #     """ Готовит аргументы для запроса модели """
    #     res = dict()
    #
    #     # key_dict = cur_data_item.key_hierarchy
    #     # di_dict = cur_data_item.get_dict()
    #     # for k, v in di_dict.items():
    #     #     res[k] = v
    #     #
    #     # for di_name in reversed(cur_data_item.parents_list()):
    #     #     temp_d = key_dict.get(di_name, dict())
    #     #     if isinstance(temp_d, dict):
    #     #         for k, v in temp_d.items():
    #     #             res[k] = v
    #     #
    #     # for k, v in additional_args.items():
    #     #     res[k] = v
    #
    #     return res

    # def __call__(self, cur_data_item: DataItemConnector, **args):
    #     args['cur_data_item'] = cur_data_item
    #     return super().__call__(**args)

    def main(self, cur_data_item, additional_args: dict, vertex_context_dict: dict,
             conn, media_master: MediaMasterAbstract):
        """ Готовит и возвращает аргументы для запроса, за выполнение основной логики (execute, commit,
            rollback... будут отвечать отдельные методы)
            conn - объект, отвечающий за физическое выпонение запроса
            media_master - обът работы с медиа
        """
        # res = self._model_connector(self._init_arg, self.get_arg(cur_data_item, additional_args))
        # result = list()
        # for d in res:
        #     result.append(self._get_item_list(d))
        # return self._prepear_change_list(result)
        # return self.get_arg(cur_data_item, additional_args)
        return None

    def get_id(self):
        """ Метод возвращает уникальный идентификатор соединения """
        if self._model_connector is not None:
            conn_id = self._model_connector.get_id()
        else:
            conn_id = None
        # модифицируем в случае необходимости conn_id с учетом специфики конкретного объекта модели
        return conn_id

    # @abstractmethod
    # def get_last_data(self, conn, arg: dict):
    #     """ Метод получения данных для возможного rollback
    #         conn - объект, отвечающий за физическое выпонение запроса
    #         arg - аргумент, как раз-таки получаемый в методе main, который в свою очередь планируется передавать
    #                 по средствам объекта-адаптера"""
    #     data = dict()
    #     return data

    # @abstractmethod
    # def execute(self, conn, media_master: MediaMasterAbstract, arg: dict, vertex_context_dict: dict):
    #     """ Метод выполнения основного запроса к модели
    #         conn - объект, отвечающий за физическое выпонение запроса
    #         media_master - обът работы с медиа
    #         arg - аргумент, как раз-таки получаемый в методе main, который в свою очередь планируется передавать
    #                 по средствам объекта-адаптера
    #     """
    #     # res = self._model_connector(self._init_arg, arg)
    #     result = list()
    #     # for d in res:
    #     #     result.append(self._get_item_list(d))
    #     return self._prepear_change_list(result)

    def rollback(self, conn, data_for_rollback):
        """ Откат операций над моделью в случае ошибки в последовательности выполнения вершин
            conn - объект, отвечающий за физическое выпонение запроса
            data_for_rollback - аргумент получаемый перед выпонением основного запроса к модели, получаемый методом
                        get_last_data и будет передан по средствам объекта-адаптера

            (Аналогичный метод есть и у ModelConnection, это другой метод,
             вызвается отдельно и содержит разный функционал)
        """
        return

    # def _get_item_list(self, line):
    #     """ Метод возвращает элемент списка, который меняет основной список блока DataItem """
    #     key = {k: v for k, v in line.items() if k in self._key_set}
    #     return key, line

    # def _prepear_change_list(self, ch_list):
    #     """ Метод производит окончательную подготовку списка изменения (добавляет/удаляет/меняет отдельные строки)"""
    #     return ch_list


class AdapterModel:
    """ Класс объекта этапа выполнения адаптер обертка для класса ModelMaster.
        Поскольку в качестве модели может быть не только база данных,
        в которой есть встроенная реализация rollback-ов, но и например системы умного дома
        или api к различным устройствам или ресурсам. В этом случае реализация возможных
        откатов в случае сбоев ложится на на объекты модели. Для того чтобы организовать откат
        объекты работы с моделью содержат специальный метод, который возвражает данные, необходимые
        для восстановления исходного состояния модели... Так вот объект AdapterModel обеспечивает
        хранение этих данных.
    """

    def __init__(self, model_master: ModelMasterAbstract, model_connection: ModelConnection,
                 cur_data_item: DataItemConnector, additional_args: dict, vertex_context_dict: dict,
                 media_master: MediaMasterAbstract):
        self._model_master = model_master
        self._data_for_rollback = model_master.main(cur_data_item=cur_data_item, additional_args=additional_args,
                                                    vertex_context_dict=vertex_context_dict,
                                                    conn=model_connection.get_conn(),
                                                    media_master=media_master)
        self._model_connection = model_connection
        self._vertex_context_dict = vertex_context_dict
        # self._last_data = model_master.get_last_data(conn=model_connection.get_conn(), arg=self._arg)

    # def execute(self, media_master: MediaMasterAbstract):
    #     return self._model_master.execute(
    #         conn=self._model_connection.get_conn(),
    #         media_master=media_master,
    #         arg=self._arg,
    #         vertex_context_dict=self._vertex_context_dict)

    def rollback(self):
        return self._model_master.rollback(
            conn=self._model_connection.get_conn(),
            data_for_rollback=self._data_for_rollback
        )


class ModelManager:
    """ Класс объекта, реализующего основную логику работы с объектами моделей и соединений """

    def __init__(self, media_master: MediaMasterAbstract):
        self._model_adapter_list = list()
        self._model_connections_dict = dict()
        self._model_connections_name_list = list()
        self._media_master = media_master

    def execute(self, model_master: ModelMasterAbstract, cur_data_item: DataItemConnector,
                additional_args: dict, vertex_context_dict: dict):
        conn_id = model_master.get_id()
        try:
            model_connection = self._model_connections_dict[conn_id]
        except KeyError:
            model_connection = ModelConnection(model_conn_id=conn_id,
                                               model_connector=model_master.get_model_connector())

            self._model_connections_name_list.append(conn_id)
            self._model_connections_dict[conn_id] = model_connection

        model_adapt = AdapterModel(model_master=model_master,
                                   model_connection=model_connection,
                                   cur_data_item=cur_data_item,
                                   additional_args=additional_args,
                                   vertex_context_dict=vertex_context_dict,
                                   media_master=self._media_master)
        # res = model_adapt.execute(media_master=self._media_master)
        self._model_adapter_list.append(model_adapt)

    def commit(self):
        self._model_adapter_list.clear()

        for k in self._model_connections_name_list:
            model_connection = self._model_connections_dict[k]
            model_connection.commit()
            model_connection.close_connection()
            del self._model_connections_dict[k]
        self._model_connections_name_list.clear()

    def rollback(self):
        # Выполняем откат по списку моделей
        for v in self._model_adapter_list:
            v.rollback()
        self._model_adapter_list.clear()

        # Выполняем откат по соединениям
        for k in self._model_connections_name_list:
            model_connection = self._model_connections_dict[k]
            model_connection.rollback()
            model_connection.close_connection()
            del self._model_connections_dict[k]
        self._model_connections_name_list.clear()


#
# Классы работы с моделью завершены
#


class ViewMaster(BaseElement):
    """ Класс отвечает за формирование непосредственно сообщения из шаблона
            А также объект принимает на вход входное представление (input view) и
            отрабатывает логику компоновки этих представлений и выдает результирующее View"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        return result

    def main(self, in_view, dataitem_protect,
             additional_args: dict, vertex_context_dict: dict):
        return in_view


class DefaultViewMaster(ViewMaster):
    _template = dict()

    def __init__(self):
        super().__init__()

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        return in_view


class PrototypeControllerMaster(BaseElement):
    """
        Класс-прототип для контроллера
    """

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    # def __call__(self, dataitem_protect, common_control_dict, msg_control_dict, **args):
    #     args['dataitem_protect'] = dataitem_protect
    #     args['common_control_dict'] = common_control_dict
    #     args['msg_control_dict'] = msg_control_dict
    #     return super().__call__(**args)

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        """
        Основной метод выполнения объекта данного класса
        Обеспечивает реализацию основной логики контроллера.
        Возвращает common_control_dict (без привязки к конкретному сообщению)
            и msg_control_dict (словарь команд с привязкой к сообщению) для соответствующего DataItem
        В процессе выполнения модифицирует входящий dataitem_protect
        """
        return dataitem_protect, common_control_dict, msg_control_dict


class ComplexControllerMaster(PrototypeControllerMaster):
    """
        Класс, обеспечивающий реализацию логики переходов между вершинами графа состояний непосредственно VertexBlob.
        Средством реализации этой логики является вставка соответствующих ссылок в DataItem
    """
    _vertex_list = list()

    def __init__(self, vertex_list=None, **args):
        """ vertex_list - список вершин для перехода ,
                            формат: [{field_name: str(...), field_caption: str(...),
                                      vertex_name: str(...), control_dict: dict(...)}, ...]
                    field_name - имя поля в представлении (опциональный параметр)
                    field_caption - значение поля в представлении (опциональный параметр)
                    vertex_name - имя вершины перехода (обязательный параметр)
                    control_dict - словарь, через который информация передается в другие вершины (обязательный параметр)
        """
        super().__init__(**args)
        args['vertex_list'] = vertex_list
        self.set_param(**args)

    def set_param(self, vertex_list=None, **args):
        if isinstance(vertex_list, list):
            for v in vertex_list:
                assert isinstance(v, dict)
                # assert set(v.keys()) == {'vertex_name', 'control_dict'}
                assert isinstance(v['control_dict'], dict)
            self._vertex_list = vertex_list

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        if vert_control_dict is not None:
            for k, v in vert_control_dict.items():
                result[k] = deepcopy(v)

        if env_dict is not None:
            for k in self._env_keys:
                try:
                    result[k] = deepcopy(env_dict[k])
                except KeyError:
                    pass

        if user_input is not None:
            for k in self._user_input_key:
                try:
                    result[k] = deepcopy(user_input[k])
                except KeyError:
                    pass

        return result


class PrototypeVertexControllerMaster(BaseElement):
    """
        Прототип контроллера вершины
    """

    # def __call__(self, control_dict, **args):
    #     args['control_dict'] = control_dict
    #     return super().__call__(**args)

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        return result

    def main(self, additional_args: dict, vertex_context_dict: dict):
        """ Метод реализует основной функционал класса
        """
        control_dict = dict()
        vertex_name = None
        return vertex_name, control_dict


class ComplexVertexControllerMaster(PrototypeVertexControllerMaster):
    """
        Класс, обеспечивающий реализацию логики переходов между вершинами
            графа состояний для Vertex (а не для VertexBlob).
        В отличие AbstractControllerMaster осуществляев переход в непосредственную вершину графа состояний
    """
    _vertex_list = list()

    def __init__(self, vertex_list=None, **args):  # , default_dict):
        """ vertex_list - список вершин для перехода ,
                        формат: [{id_vert: str(...), vertex_name: str(...), command_dict: dict(...)}, ...]
                id_vert - строковое имя-идентификатор перехода для внутренних нужд, в основном для использования
                            в представлении
                vertex_name - имя вершины перехода
                control_dict - словарь, через который информация передается в другие вершины
            default_dict - словарь значений по умолчанию
        """
        super().__init__(**args)
        args['vertex_list'] = vertex_list
        self.set_param(**args)

    def set_param(self, **args):

        try:
            temp = args['vertex_list']
        except KeyError:
            pass
        else:
            if isinstance(temp, list):
                self._vertex_list = temp

    # def get_dict(self, arg):
    #     """ Получаем словарь значений """
    #     res = deepcopy(self._default_dict)
    #     for k, v in arg.items():
    #         res[k] = deepcopy(v)
    #     return res

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        if vert_control_dict is not None:
            for k, v in vert_control_dict.items():
                result[k] = deepcopy(v)

        if env_dict is not None:
            for k in self._env_keys:
                try:
                    result[k] = deepcopy(env_dict[k])
                except KeyError:
                    pass

        if user_input is not None:
            for k in self._user_input_key:
                try:
                    result[k] = deepcopy(user_input[k])
                except KeyError:
                    pass

        return result

    def main(self, additional_args: dict, vertex_context_dict: dict):
        """ Метод реализует основной функционал класса
        """
        control_dict = dict()
        vertex_name = None
        return vertex_name, control_dict


class DictIdGetter(BaseElement):
    """
        Класс объектов, предназначенных для получения адреса для отправки нового сообщения
        на вход вводит dict_id, msg_id и DataItemProtect
    """

    def __init__(self, **args):
        super().__init__(**args)

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    # def __call__(self, control_dict, dict_id_in, msg_id_in, **args):
    #     args['control_dict'] = control_dict
    #     args['dict_id_in'] = dict_id_in
    #     args['msg_id_in'] = msg_id_in
    #     return super().__call__(**args)

    def main(self, dict_id_in, msg_id_in, additional_args: dict, vertex_context_dict: dict):
        """ dict_id_in - словарь,
            msg_id_in - id сообщения,
            arg_dict - словарь аргументов. полученный с помощью ControlDictSwitch
        """
        dict_id, msg_id = dict_id_in, msg_id_in
        return dict_id, msg_id


class WaitSignalSetter(BaseElement):
    """
        Класс объекта этапа инициализации, который отвечает на вопрос (возвращает True или False)
        нужно ли останавливаться после выполнения Vertex для ожидания ответа пользователя
    """

    def __init__(self, **args):
        super().__init__(**args)

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    # def __call__(self, control_dict, dict_id_in, msg_id_in, **args):
    #     args['dict_id_in'] = dict_id_in
    #     args['msg_id_in'] = msg_id_in
    #     args['control_dict'] = control_dict
    #     return super().__call__(**args)

    def main(self, dict_id_in, msg_id_in, additional_args: dict, vertex_context_dict: dict):
        return True


class PrototypeFullVertexControllerMaster(BaseElement):
    """
        Прототип полного контроллера вершины (результат PrototypeVertexControllerMaster + WaitSignalSetter)
    """

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()
        return result

    def main(self, dict_id_in, msg_id_in, additional_args: dict, vertex_context_dict: dict):
        """ Метод реализует основной функционал класса
        """
        control_dict = dict()
        vertex_name = None
        wait_flag = True
        return wait_flag, vertex_name, control_dict


class PrototypeManagedMsgController(BaseElement):
    """ Класс объекта обработки управляющих сообщений, возвращает команды управления
            и множеств, соответствующих алиасов сообщений, к которым необходимо применить эти команды """
    _vertex_list = list()

    def __init__(self, vertex_list=None, **args):  # , default_dict):
        """ vertex_list - список вершин для обработки "управляемых сообщений",
                        формат: [{id_vert: str(...), vertex_name: str(...), command_dict: dict(...)}, ...]
                id_vert - строковое имя-идентификатор перехода для внутренних нужд, в основном для использования
                            в представлении
                vertex_name - имя вершины перехода
                control_dict - словарь, через который информация передается в другие вершины
            default_dict - словарь значений по умолчанию
        """
        super().__init__(**args)
        args['vertex_list'] = vertex_list
        self.set_param(**args)

    def set_param(self, **args):
        try:
            temp = args['vertex_list']
        except KeyError:
            pass
        else:
            if isinstance(temp, list):
                self._vertex_list = temp

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()

        result[self._alias_msg_name] = alias_msg
        return result

    # def __call__(self, dataitem_protect, **args):
    #     args['dataitem_protect'] = dataitem_protect
    #     return super().__call__(**args)

    def main(self, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        """
            return list(tuple(alias_msg_1, vertex_name_1, control_dict_1),
                        tuple(alias_msg_2, vertex_name_2, control_dict_2))
        """
        return list()


class VertexBlob:
    """ Базовый элемент графа состояний
        В терминах модели MCV - является контроллером (не смотря на то, что в модель MCV добавлен еще один элемент
        - Состояние, в этом смысле модель будет называться уже MCVS)

        на вход принимает:
                - управляющие аргументы,
                - объект полностью характеризующий сообщение
        выдает:
                - управляющие аргументы для аналогичного DataItem
                - объект полностью характеризующий сообщение (текст, картинку, клавиатуру ....
                    измененный либо тот же) (для следующего объекта в цепочке)
        выполняет:
                - выполняет действие модели изменяя при этом состояние
                    выполняет построение представления

    """
    _local_session_builder = DataItemConnectorBuilder(None, None)
    _view_keeper = ViewMaster()
    _controller_keeper = PrototypeControllerMaster()
    _control_dict_switch_to_vertex_def = dict()

    _UPDATE_DATA_ITEM_FOR_MSG = '_update_di'
    _DELETE_DATA_ITEM_FOR_MSG = '_delete_di'

    def __init__(self, local_session_builder=None, model_keeper=None, view_keeper=None,
                 controller_keeper=None, control_dict_switch=None, managed_msg_controller=None,
                 control_dict_switch_to_vertex=None):
        """ local_session_builder - объект-хранитель состояния из MCVS (model-view-controller-state) DataItemConnector
                           класс DataItemConnectorCreator
            model_keeper - объект-хранитель модели из MCVS, обеспечивает соответствующие интерфейсы для доступа к
                           хранилищу данных.
                           класс ModelMaster
            view_keeper - объект-хранитель представления из MCVS, обеспечивает соответствующий интерфейс абстрагируясь
                           от конкретной реализации
                           класс ViewMaster
            controller_keeper - объект хранитель контроллера из MCVS, обеспечивает наполнение Представления логикой и
                            выполнения переходов между вершинами графа состояний
                            класс-потомок AbstractControllerMaster
            control_dict_switch - коммутатор для входных управляющих данных, класс ControlDictSwitch

            managed_msg_controller - контроллер управляемых сообщений (Наследник PrototypeManagedMsgController)
            control_dict_switch_to_vertex - объект класса DictSwitcherVB для формирования словаря передачи
                                в объект класса VertexControllerMaster
            """
        assert isinstance(local_session_builder, DataItemConnectorBuilder) or local_session_builder is None
        assert isinstance(model_keeper, ModelMasterAbstract) or model_keeper is None
        assert isinstance(view_keeper, ViewMaster) or view_keeper is None
        assert isinstance(controller_keeper, PrototypeControllerMaster) or controller_keeper is None
        assert isinstance(control_dict_switch, DictSwitcherVB) or control_dict_switch is None
        assert isinstance(managed_msg_controller, PrototypeManagedMsgController) or managed_msg_controller is None
        assert isinstance(control_dict_switch_to_vertex, DictSwitcherVB) or control_dict_switch_to_vertex is None

        if local_session_builder is not None:
            self._local_session_builder = local_session_builder
        self._model_keeper = model_keeper
        if view_keeper is not None:
            self._view_keeper = view_keeper
        if controller_keeper is not None:
            self._controller_keeper = controller_keeper
        self._control_dict_switch = control_dict_switch
        self._managed_msg_controller = managed_msg_controller
        self._control_dict_switch_to_vertex = control_dict_switch_to_vertex

    def get_name(self):
        """
            Возвращает имя выходящего DataItem для DataItemConnector
        """
        return self._local_session_builder.get_name()

    def __call__(self, session_manager_collect: SessionManagerCollection,
                 model_manager, user_input, vert_control_dict, common_control_dict,
                 msg_control_dict, view_in, alias_msg, vertex_context_dict, env_dict=None):
        """
            :param session_manager_collect: коллекция хранилищ состояний, которое каждый раз создается вновь
                        на этапе выполнения
            :param model_manager: хранилище соединений с моделью, которое каждый раз сосдается вновь на этапе выполнения
                            для возможности централизованного управления соединениями или отката в случае надобности
            :param user_input: данные введеные пользователем (для входа control_dict_switch)
            :param common_control_dict: и
            :param msg_control_dict: Управляющие данные сформированные Контроллером
                                ранее при обработки этого блока (для входа control_dict_switch)
            # control_dict - управляющие данные сформированные Контроллером
            #                     ранее при обработки этого блока (для входа control_dict_switch)
            :param vert_control_dict: управляющие данные вершины сформированные предыдущим Контроллером вершины
                    (контроллер вершины - потомок класса AbstractVertexControllerMaster)
                                (для входа control_dict_switch)
            :param view_in: сформированное сообщение предыдущим VertexBlob
            :param alias_msg: алиас сообщения
            :param env_dict: словарь, содержащий данные окружения бота (имя бота, инфо о боте, инфо о чате)
            :param vertex_context_dict:
        """

        # Инициализация объектов этапа выполнения (на основе соответствующих объектов этапа инициализации (Creators))
        session_manager = session_manager_collect.get_session()
        local_session_keeper = self._local_session_builder(session_manager)

        # готовим словарь аргументов для элементов VertexBlob
        arg_elements = dict(user_input=user_input,
                            vert_control_dict=vert_control_dict,
                            env_dict=env_dict,
                            alias_msg=alias_msg)

        # Записываем данные в локальную сессию перед выполнением модели (если она конечно есть)
        if self._control_dict_switch is not None:
            additional_args = self._control_dict_switch.switch(**arg_elements)
            control_dict = self._control_dict_switch.main(
                data_item=local_session_keeper,
                additional_args=additional_args,
                vertex_context_dict=vertex_context_dict
            )
            # local_session_keeper.append_dict(control_dict)

        # Выполняем модель (если она задана)
        if self._model_keeper is not None:
            local_session_keeper.key_set = self._model_keeper.key_set  # свойство local_session_keeper.key_set
            # запишется только если оно было пустым
            additional_args = self._model_keeper.switch(**arg_elements)
            model_manager.execute(
                model_master=self._model_keeper,
                cur_data_item=local_session_keeper,
                additional_args=additional_args,
                vertex_context_dict=vertex_context_dict)
            # local_session_keeper.perform_model(change_list)

        # Фиксируем изменения
        local_session_keeper.embed()
        # TODO: метод embed() выдает списки алиасов сообщений, соответствующих
        #       удаленным и измененным вершинам, необходимо обработать эти списки,
        #       должны вызываться соответствующие обработчики, для сообщений, использующих данные data_item

        # Выполняем контроллер
        # dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict, **args
        additional_args = self._controller_keeper.switch(**arg_elements)
        res_tuple = self._controller_keeper.main(
            dataitem_protect=local_session_keeper,
            common_control_dict=common_control_dict,
            msg_control_dict=msg_control_dict,
            additional_args=additional_args,
            vertex_context_dict=vertex_context_dict
        )

        local_session_keeper, common_control_dict, msg_control_dict = res_tuple

        # Выполняем объект представления
        additional_args = self._view_keeper.switch(**arg_elements)
        view_out = self._view_keeper.main(
            view_in,
            local_session_keeper,
            additional_args=additional_args,
            vertex_context_dict=vertex_context_dict
        )

        # Находим список управляемых сообщений и что с ними нужно сделать (действия, которые необходимо
        # предпринять над управляемыми сообщениями определяются командой)
        if self._managed_msg_controller is not None:
            additional_args = self._managed_msg_controller.switch(**arg_elements)
            managed_msg_list = self._managed_msg_controller.main(
                dataitem_protect=local_session_keeper,
                additional_args=additional_args,
                vertex_context_dict=vertex_context_dict
            )
        else:
            managed_msg_list = list()

        if self._control_dict_switch_to_vertex is not None:
            additional_args = self._control_dict_switch_to_vertex.switch(**arg_elements)
            for_control_dict_vert = self._control_dict_switch_to_vertex.main(
                data_item=local_session_keeper,
                additional_args=additional_args,
                vertex_context_dict=vertex_context_dict
            )
        else:
            for_control_dict_vert = self._control_dict_switch_to_vertex_def

        # добавление в список управляемых сообщений, алиасов соответствующих
        # удаленным или обновленным data_item
        # _upd_alias_list, _del_alias_list = local_session_keeper.get_del_upd_alias_lists()
        # local_session_keeper.add_cur_id()
        #
        # for v in _upd_alias_list:
        #     managed_msg_list.append((v, self._UPDATE_DATA_ITEM_FOR_MSG, dict()))
        # for v in _del_alias_list:
        #     managed_msg_list.append((v, self._DELETE_DATA_ITEM_FOR_MSG, dict()))

        return common_control_dict, msg_control_dict, view_out, \
               managed_msg_list, for_control_dict_vert


class Vertex:
    """
        Основной Объект вершины. Объект создается на этапе инициализации системы.
    """
    _controller_vert_keeper = None
    _dict_id_getter = DictIdGetter()
    _wait_signal_setter = None
    _wait_flag_default = True
    # _next_control_dict_def = dict()
    _next_vertex_name_def = None
    _controller_vert_full = None

    def __init__(self, vert_blob_list=None, controller_vert_keeper=None,
                 dict_id_getter=None, wait_signal_setter=None, wait_flag_default=True,
                 next_vertex_name_def=None, next_control_dict_def=None, controller_vert_full=None):
        self._vert_blob_list = list()
        kwargs = dict()
        kwargs['vert_blob_list'] = vert_blob_list
        kwargs['controller_vert_keeper'] = controller_vert_keeper
        kwargs['dict_id_getter'] = dict_id_getter
        kwargs['wait_signal_setter'] = wait_signal_setter
        kwargs['wait_flag_default'] = wait_flag_default
        kwargs['next_vertex_name_def'] = next_vertex_name_def
        kwargs['next_control_dict_def'] = next_control_dict_def
        kwargs['controller_vert_full'] = controller_vert_full
        self.update_vert(**kwargs)

    def update_vert(self, vert_blob_list=None, controller_vert_keeper=None,
                    dict_id_getter=None, wait_signal_setter=None, wait_flag_default=None,
                    next_vertex_name_def=None, next_control_dict_def=None,
                    controller_vert_full=None):
        """ функционал вынесен в отдельный метод специально для билдера """
        # TODO: Убрать все эти None по умолчанию и *_default (3 последних параметра) и сделать,
        #       чтоб builder вставлял какие-то осмысленные объекты
        """
            vert_blob_list - список элемнтов VertexBlob
            controller_vert_keeper - контроллер вершины (потомок класса AbstractVertexControllerMaster )
                                        задача: выдать следующую вершину и управляющие параметры
            dict_id_getter - объект для получения
            wait_signal_setter - объект, отвечающий на вопрос нужно или нет остановиться
                                    и подождать ответа пользователя
            wait_flag_default - флаг останова, если wait_signal_setter - None
            next_vertex_name_def - следующая вершина, если controller_vert_keeper==None
            next_control_dict_def - управляющие данные для следующей вершины. если next_control_dict_def==None
        """
        assert isinstance(controller_vert_keeper, PrototypeVertexControllerMaster) \
               or controller_vert_keeper is None
        assert isinstance(vert_blob_list, list) or vert_blob_list is None
        assert isinstance(dict_id_getter, DictIdGetter) or dict_id_getter is None
        assert isinstance(wait_signal_setter, WaitSignalSetter) or wait_signal_setter is None
        assert isinstance(wait_flag_default, bool) or wait_flag_default is None
        assert isinstance(next_control_dict_def, dict) or next_control_dict_def is None
        assert isinstance(next_vertex_name_def, str) or next_vertex_name_def is None
        assert isinstance(controller_vert_full, PrototypeFullVertexControllerMaster) \
               or controller_vert_full is None

        if vert_blob_list is not None:
            for v in vert_blob_list:
                assert isinstance(v, VertexBlob)
                self._vert_blob_list.append(v)

        if controller_vert_keeper is not None:
            self._controller_vert_keeper = controller_vert_keeper
        if dict_id_getter is not None:
            self._dict_id_getter = dict_id_getter
        if wait_signal_setter is not None:
            self._wait_signal_setter = wait_signal_setter
        if wait_flag_default is not None:
            self._wait_flag_default = wait_flag_default
        if next_control_dict_def is not None:
            self._next_control_dict_def = next_control_dict_def
        else:
            self._next_control_dict_def = dict()
        if next_vertex_name_def is not None:
            self._next_vertex_name_def = next_vertex_name_def
        if controller_vert_full is not None:
            self._controller_vert_full = controller_vert_full

        return self

    @property
    def vert_blob_list(self):
        return self._vert_blob_list

    @vert_blob_list.setter
    def vert_blob_list(self, vert_blob_list):
        self._vert_blob_list = vert_blob_list

    def __call__(self, session_manager_collect, model_manager, msg_pusher,
                 user_input, vert_control_arg, env_dict):
        """
        session_manager_collect - коллекция хранилищ состояния, которое каждый раз создается вновь на этапе выполнения
        model_manager - хранилище соединений с моделью, которое каждый раз сосдается вновь на этапе выполнения
                        для возможности централизованного управления соединениями или отката в случае надобности
        msq_pusher - объект, отвечающий за отправку и хранение данных сообщения
        command - команда (при вводе ссылки (вида: /команда ) или нажатики клавиши на клавиатуре)
                    (данный аргумент не актуален, поскольку всю информацию содержит msq_pusher)
        user_input - ввод от пользователя (словарь)
        vert_control_arg - словарь от предыдущего элемента Vertex
        env_dict - словарь, содержащий данные окружения для бота
        return: вершина для перехода и управляющие данные (vert_control_arg для следующего элемента vertex)
        """

        assert isinstance(msg_pusher, ContextConnector)
        # assert isinstance(command, str)
        assert isinstance(vert_control_arg, dict)  # or vert_control_arg is None

        vert_control_arg = deepcopy(vert_control_arg)
        alias_msg = msg_pusher.msg_alias

        # готовим словарь аргументов для элементов VertexBlob
        arg_elements = dict(user_input=user_input,
                            vert_control_dict=vert_control_arg,
                            env_dict=env_dict,
                            alias_msg=alias_msg)
        vertex_context_dict = dict()  # словарь, связывающий в общее информационное
        # пространство все составные объекты вершины Vertex,
        # передается в качестве параметра в каждый объект

        # Получаем заготовку сообщения
        view_ = msg_pusher.msg_msg_obj
        common_control_dict = msg_pusher.msg_common_dict
        msg_control_dict = msg_pusher.msg_msg_dict
        managed_msg_list = list()
        res_control_dict = dict()  # Результирующий словарь, сформированный на основе всех
        # словарей for_control_dict всех элементов из vert_blob_list

        try:
            for v in self._vert_blob_list:
                # session_manager, user_input, vert_control_dict, common_control_dict,
                #                  msg_control_dict, view_in, env_dict=None
                common_control_dict, msg_control_dict, view_, local_managed_msg_dict, for_control_dict = v(
                    session_manager_collect=session_manager_collect,
                    model_manager=model_manager,
                    user_input=user_input,
                    vert_control_dict=vert_control_arg,
                    common_control_dict=common_control_dict,
                    msg_control_dict=msg_control_dict,
                    view_in=view_,
                    alias_msg=alias_msg,
                    vertex_context_dict=vertex_context_dict,
                    env_dict=env_dict)
                managed_msg_list.extend(local_managed_msg_dict)
                # for kk, vv in for_control_dict.items():
                #     res_control_dict[kk] = vv
        except LogicalModelError:
            return False, self._next_vertex_name_def, self._next_control_dict_def, managed_msg_list

        msg_pusher.msg_common_dict = common_control_dict
        msg_pusher.msg_msg_dict = msg_control_dict
        msg_pusher.msg_msg_obj = view_

        # словарь аргументов для DictSwitcher
        # switch_arg = dict(session_manager=session_manager,
        #                   user_input=user_input,
        #                   vert_control_dict=vert_control_arg,
        #                   env_dict=env_dict)

        # находим новые значение dict_id и msg_id
        if self._dict_id_getter is not None:
            additional_args = self._dict_id_getter.switch(**arg_elements)
            new_dict_id, new_msg_id = self._dict_id_getter.main(
                dict_id_in=msg_pusher.msg_dict_id,
                msg_id_in=msg_pusher.msg_msg_id,
                additional_args=additional_args,
                vertex_context_dict=vertex_context_dict
            )
            # id_dict = dict(dict_id=new_dict_id, msg_id=new_msg_id)
            msg_pusher.msg_dict_id = new_dict_id
        # else:
        #     id_dict = None

        # print(f'\__/ id_dict = {id_dict}, type(self._dict_id_getter) = {type(self._dict_id_getter)}')
        # msg_pusher.full_msg_id(id_dict)
        # записываем, но не отправляем сообщение, отправлено оно будет
        # в основном цикле DialogMachine
        # msq_pusher.msg_send()

        if self._controller_vert_full is None:
            # находим флаг ожидания (wait_flag = True - если надо остановиться,
            # иначе пройти дальше)
            if self._wait_signal_setter is not None:
                additional_args = self._wait_signal_setter.switch(**arg_elements)
                wait_flag = self._wait_signal_setter.main(
                    dict_id_in=msg_pusher.msg_dict_id,
                    msg_id_in=msg_pusher.msg_msg_id,
                    additional_args=additional_args,
                    vertex_context_dict=vertex_context_dict
                )
            else:
                wait_flag = self._wait_flag_default

            # Находим следующую вершину и vert_control_arg для нее
            if self._controller_vert_keeper is not None:
                additional_args = self._controller_vert_keeper.switch(**arg_elements)
                res = self._controller_vert_keeper.main(
                    additional_args=additional_args,
                    vertex_context_dict=vertex_context_dict
                )
                next_vertex_name, next_control_dict = res[0], res[1]
            else:
                next_vertex_name, next_control_dict = self._next_vertex_name_def, self._next_control_dict_def
        else:
            additional_args = self._controller_vert_full.switch(**arg_elements)
            wait_flag, next_vertex_name, next_control_dict = self._controller_vert_full.main(
                dict_id_in=msg_pusher.msg_dict_id,
                msg_id_in=msg_pusher.msg_msg_id,
                additional_args=additional_args,
                vertex_context_dict=vertex_context_dict
            )

        return wait_flag, next_vertex_name, next_control_dict, managed_msg_list

        # if isinstance(view_, dict) and len(view_) > 0:
        #     session_manager_collect.put_cur_data_item_list(alias_msg=alias_msg)
        # else:
        #     session_manager_collect.put_cur_data_item_list(alias_msg=None)

        # return wait_flag, res[0], res[1], managed_msg_list


class DialogMachineCore:
    """ Ядро главного объекта организации диалога
        Это значит, что объект этого класса дожен находиться внутри объекта-адаптера,
        разработанного под определенную платформу и предоставлять
        свой функционал """

    _MAX_COUNTER = 50
    CHANGE_NONE_MSG = '_change_none_msg'
    MANAGED_LIST_ITERATIONS = 2

    def __init__(self, context_storage, state_machine, msg_getter, sender_msg, msg_obj_default,
                 media_master: MediaMasterAbstract):
        """
            context_storage - объект, реализующий хранение текущего состояния
            sender_msg - объект, отвечающий за отправку сообщений
            msg_getter - хранение информации для этих сообщений
            state_machine - объект-хранитель графа состояния
            msg_obj_default - объект сообщения по умолчанию (шаблон сообщения со всеми
                                ключевыми полями. которые будет использоваться)
            media_master - объект доступа к медиа
        """
        assert isinstance(context_storage, ContextStorage)
        assert isinstance(state_machine, StateMachine)
        assert isinstance(msg_getter, MsgGetter)
        assert isinstance(sender_msg, SenderMsg)
        assert isinstance(msg_obj_default, dict)
        assert isinstance(media_master, MediaMasterAbstract)

        self._context_storage = context_storage
        self._state_machine = state_machine
        self._msg_getter = msg_getter
        self._sender_msg = sender_msg
        self._msg_obj_default = msg_obj_default
        self._media_master = media_master

    @staticmethod
    def get_hash(value):
        """ Находит hash любого объекта python, необходимо для короткого ключа redis """
        return hashlib.md5(pformat(value).encode()).hexdigest()

    def start_command_test(self, dict_id, alias_msg, param, env_dict=None):
        """ Вызывается при инициациировании диалога пользователем
            alias_msg -
            param - параметры при команде /start
            env_dict - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        self.process(dict_id, None, alias_msg, '/start', param, env_dict)

    def process_command_test(self, dict_id, msg_id, command, param, env_dict=None):
        """ При вызове команды
            msg_id - id входящего сообщения от пользователя
                    (только номер сообщения - остальные данные есть в dict_id)
            param - параметры при команде command
            env_dict - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        self.process(dict_id, None, None, command, param, env_dict)

    def process_msg_test(self, dict_id, msg_id, user_input, env_dict=None):
        """ При вызове сообщения
            msg_id - id входящего сообщения от пользователя
                    (только номер сообщения - остальные данные есть в dict_id)
            user_input - пользовательский ввод (словарь с элементами: текстом, файлами,
                            изображениями)
            env_dict - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        self.process(dict_id, None, None, None, user_input, env_dict)

    def process_callback_test(self, dict_id, msg_id, callback, param, env_dict=None):
        """ При обработке callback от сообщения
            msg_id - id сообщения бота, от которого пришел ответ
            param - параметры при callback, если есть
            env_dict - данные окружения для бота (имя бота, данные бота, данные чатов...)
        """
        self.process(dict_id, msg_id, None, callback, param, env_dict)

    def process(self, dict_id, msg_id, alias_msg, command, param, env_dict=None):
        """
            Главный метод обработки сообщений
            dict_id - словарь адреса диалога
            msg_id - идентификатор сообщения (может быть None) (сообщения бота , от которого пришел ответ)
            alias_msg - идентификатор сообщения, который может заменить dict_id и msg_id
            command - команда или callback
            param - пользовательский ввод (словарь с элементами: текстом, файлами, изображениями)
            env_dict - данные окружения для бота (имя бота, данные бота, данные чатов... - та информация,
                        которая не зависит от конкретного сообщения)
        """
        # TODO: Вернуть все обработчики ошибок на место после отладок
        # Создаем адаптер для контекста
        # try:
        start_control_param = dict(
            vertex_name=self._state_machine.get_begin_vertex_name(),
            control_dict=dict(),
            var_state=self._state_machine.get_begin_var_state())
        context_connector = ContextConnector(
            # context_store=self._context_storage,
            msg_sender=self._sender_msg,
            msg_getter=self._msg_getter,
            msg_obj_default=self._msg_obj_default,
            start_control_param=start_control_param
        )
        vertex_name, control_dict = context_connector.init_main_context(
            dict_id=dict_id,
            msg_id=msg_id,
            command=command,
            alias_msg=alias_msg)
        # except Exception as e:
        #     logging.warning('Error in DialogMachine: {}'.format(e))
        #     del context_connector
        #     return

        # if context_connector.vertex_name is None:
        #     context_connector.set_control_data(self._state_machine.get_begin_vertex_name(), dict())
        #     context_connector.var_state = self._state_machine.get_begin_var_state()
        # session_manager_collect = SessionManager(context_connector.var_state)
        session_manager_collect = SessionManagerCollection(
            main_dict_id=context_connector.main_dict_id,
            context_store=self._context_storage
        )
        model_manager = ModelManager(media_master=self._media_master)
        # var_state_master = VarStateMaster(var_state=context_connector.var_state, dict_id=dict_id, msg_id=msg_id)

        wait_flag = False
        # cycle_fix_set = set()  # хранилище vertex_name для предотвращения циклов
        # # cycle_fix_set.add(vertex_name)
        # cycle_fix_set.add(self.get_key((vertex_name, dict_id)))
        logging.debug(" - -- DialogMachineCore vertex_name={}".format(vertex_name))

        managed_msg_list = list()

        msg_counter = 1

        while not wait_flag:
            cur_vert: Vertex = self._state_machine.get_vertex_by_name(vertex_name)
            logging.debug(
                " - -- DialogMachineCore vertex_name1={}".format(self._state_machine.get_name_by_vertex(cur_vert)))
            try:
                wait_flag, vertex_name, control_dict, loc_managed_msg_dict = cur_vert(
                    session_manager_collect=session_manager_collect,
                    model_manager=model_manager,
                    msg_pusher=context_connector,
                    user_input=param,
                    vert_control_arg=control_dict,
                    env_dict=env_dict)

                managed_msg_list.extend(loc_managed_msg_dict)
            except Exception as e:
                logging.error("Exception in main cycle: {}".format(e))
                model_manager.rollback()
                del session_manager_collect
                del context_connector
                return

            # dict_id = context_connector.msg_dict_id
            context_connector.msg_send(vertex_name=vertex_name,
                                       control_dict=control_dict)

            logging.debug(" - - DialogMachineCore vertex_name2={} control_dict={}, wait_flag={}".format(
                vertex_name,
                control_dict,
                wait_flag))

            if vertex_name is None:
                wait_flag = True

            if not wait_flag and self._MAX_COUNTER > 0:
                if msg_counter > self._MAX_COUNTER:
                    model_manager.rollback()
                    raise DialogMachineError("Msg counter > {}, vertex_name = {}".format(self._MAX_COUNTER,
                                                                                         vertex_name))
                else:
                    msg_counter += 1

        # if not wait_flag:
        #     # if vertex_name is None:
        #     #     var_state_master = VarStateMaster(var_state=context_connector.var_state)
        #     # vertex_name = context_connector.vertex_name
        #     # control_dict = context_connector.control_dict
        #
        #     # Предотвращаем бесконечный цикл (если эта вершина уже была и wait_flag == False,
        #     # то необходимо выбросить ошибку)
        #     hash_4_cycle = self.get_key((vertex_name, dict_id))
        #     if hash_4_cycle in cycle_fix_set:
        #         model_manager.rollback()
        #         raise DialogMachineError("Cycle in dialog, vertex '{}'".format(vertex_name))
        #     else:
        #         # cycle_fix_set.add(vertex_name)
        #         cycle_fix_set.add(hash_4_cycle)

        context_connector.set_context()

        # Обработка изменения сообщения по-умолчанию, если оно было изменено и не обработано
        alias_def_msg_changed = context_connector.get_changed_def_alias_msg
        if alias_def_msg_changed is not None:
            try:
                vertex_name, control_dict = context_connector.init_controlled_context(
                    alias_def_msg_changed,
                    command=self.CHANGE_NONE_MSG)
                if vertex_name is None:
                    raise NoSuchMsgError('Error vertex_name is None')
            except Exception as e:
                # В случае, если возникли проблемы на этапе получения данных сообщения по-умолчанию,
                # - просто игнорируем эту обработку
                logging.debug(
                    f" - DialogMachineCore error in handling def_msg e={e}")
                pass
            else:
                cur_vert = self._state_machine.get_vertex_by_name(vertex_name)
                logging.debug(
                    " - -- DialogMachineCore vertex_name1={}".format(
                        self._state_machine.get_name_by_vertex(cur_vert)))
                try:
                    wait_flag_t, vertex_name_t, control_dict_t, loc_managed_msg_dict = cur_vert(
                        session_manager_collect=session_manager_collect,
                        model_manager=model_manager,
                        msg_pusher=context_connector,
                        user_input=param,
                        vert_control_arg=control_dict,
                        env_dict=env_dict)
                    context_connector.simple_msg_send(to_set_none_el=False)
                    managed_msg_list.extend(loc_managed_msg_dict)
                    context_connector.set_context()
                except Exception as e:
                    logging.error("Exception in change def msg handler: {}".format(e))
                    model_manager.rollback()
                    del context_connector
                    del session_manager_collect
                    return

        # Обработка списка управляемых данных (managed_msg_dict)
        for ii in range(self.MANAGED_LIST_ITERATIONS):
            if len(managed_msg_list) == 0:
                break
            temp_list = list()
            for cur_alias, cur_vertex_name, cur_control_dict in managed_msg_list:
                # Получаем управляемое сообщение
                try:
                    context_connector.init_controlled_context(alias_msg=cur_alias)
                except Exception as e:
                    # context_connector.clear_context_store()
                    continue
                else:
                    cur_vert = self._state_machine.get_vertex_by_name(cur_vertex_name)
                    try:
                        # Поскольку для управляемых сообщений необходимо изменить только одно сообщение,
                        # то получаемые параметры от выполняемой вершины - неважны
                        wait_flag_m, vertex_name_m, control_dict_m, loc_managed_list = cur_vert(
                            session_manager_collect=session_manager_collect,
                            model_manager=model_manager,
                            msg_pusher=context_connector,
                            user_input=param,
                            vert_control_arg=cur_control_dict,
                            env_dict=env_dict)
                    except Exception as e:
                        # context_connector.clear_context_store()
                        logging.warning('Any disaster in managed msg handling e={}'.format(e))
                        continue
                    else:
                        temp_list.extend(loc_managed_list)
                        context_connector.simple_msg_send()
                        context_connector.set_context()
            managed_msg_list = temp_list
        context_connector.send_msg_list()

        # Очищаем все сессии из session_manager_collect
        # от  неглобальных элементов
        session_manager_collect.del_not_permanent()
        # Сохраняем сессии
        session_manager_collect.set_sessions()

        # Коммитим список моделей
        model_manager.commit()


if __name__ == "__main__":
    logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s] %(message)s', level=logging.DEBUG)
