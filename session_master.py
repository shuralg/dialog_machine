# -*- coding: utf-8 -*-

from copy import deepcopy, copy
from dialog_machine.project_exceptions import *
from dialog_machine.interfaces import ContextStorage, DataItemInterface, ContextItem
from pprint import pformat
import hashlib
from collections import OrderedDict


def get_key(value):
    """ Находит hash любого объекта python, необходимо для короткого ключа redis """
    # return str(value['chat_id']) + str(value['user_id'])
    return hashlib.md5(pformat(value).encode()).hexdigest()


class DataItem:
    """ Базовый элемент контекста, объединяет в себе таблицу из модели (реализуется через OrderedDict);
        словарь - элемент таблицы, связь с таблицей осуществляется через cursor;
        список ключевых полей (актуален как для таблицы так и для словаря)
    """

    def __init__(self, name, parent=None, children_set=None, key_set=None, is_permanent=False):
        self.name = name
        self._table = OrderedDict()

        self.key_set = key_set
        self._dict = dict()

        # Значение None означает, что связь между
        # self.table и self.cur_dict не установлена, отсчет cursor ведется с 0
        self._cursor = None
        self._key_cursor = None
        self.parent = parent
        self.children_set = children_set if children_set is not None else set()

        # Флаг, показывающий удалять ли DataItem после сессии или элемент постоянный
        self.is_permanent = is_permanent

        # Флаг, показывающий обновлен ли главный словарь self._dict
        self._is_updated = False

    def get_key(self, value):
        if self.key_set is None:
            return None
        return get_key({k: value.get(k, None) for k in self.key_set})

    def get_current_key(self):
        if self.key_set is None:
            return None
        return {k: self._dict.get(k, None) for k in self.key_set}

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, value):
        if value is None:
            self._cursor = None
        elif type(value) is not int:
            self._cursor = None
        elif len(self._table) == 0:
            self._cursor = None
        elif len(self._table) <= value:
            self._cursor = len(self._table) - 1
        elif value < 0:
            self._cursor = 0
        else:
            self._cursor = value
        if self._cursor is not None:
            self._key_cursor = list(self._table.keys())[self._cursor]
            self._dict.clear()
            self._dict = deepcopy(self._table[self._key_cursor])
        else:
            self._key_cursor = None

    def get_cursor_by_value(self, value: dict):
        """
        Получаем номер строки в таблице table по value
        :param value:
        :return:
        """
        i = None
        if self.key_set is not None and self.key_set == set(value.keys()):
            key = self.get_key(value)
            for i, k in enumerate(self.table.keys()):
                if key == k:
                    break
        else:
            for i, val in enumerate(self.table.values()):
                flag = True
                for k, v in value.items():
                    try:
                        if v != val[k]:
                            flag = False
                            break
                    except KeyError:
                        flag = False
                        break
                if flag:
                    break

        return i

    # Интерфейс доступа к таблице self._table
    def ins_upd_in_table(self, value):
        i = None
        if self.key_set is not None:
            key = self.get_key(value)
            try:
                self._table[key]
            except KeyError:
                i = len(self._table)
            else:
                for i, k in enumerate(self._table.keys()):
                    if k == key:
                        break

            self._table[key] = value
            if key == self._key_cursor:
                self._dict = deepcopy(self._table[self._key_cursor])
            elif self.cursor is None:
                self.cursor = i
            elif self._is_updated:
                self.cursor = self.cursor
            self._is_updated = False
        else:
            key = self.cursor
            if key is None:
                key = len(self._table)
            self._table[key] = value
            self.cursor = i = key
        return i

    def del_in_table(self, value=None):
        i = None
        if self.key_set is not None:
            key = self.get_key(value)
            try:
                self._table[key]
            except KeyError:
                pass
            else:
                for i, k in enumerate(self._table.keys()):
                    if k == key:
                        break
                del self._table[key]
                if key == self._key_cursor:
                    self.cursor = self.cursor
                    if self.cursor is None:
                        self._dict.clear()
                elif self.cursor is not None and i < self.cursor:
                    self.cursor = self.cursor - 1
                elif self._is_updated:
                    self.cursor = self.cursor
                self._is_updated = False
        else:
            if value is not None:
                self.cursor = value
            key = self.cursor
            if key is not None:
                del self._table[key]
                self._table = OrderedDict([(i, v) for i, v in enumerate(self._table.values())])
                self.cursor = i = key

        return i

    def clear_table(self):
        self._table.clear()
        self.cursor = 0

    # Интерфейсы для доступа к основному словарю self._dict
    @property
    def main_dict(self):
        return deepcopy(self._dict)

    @main_dict.setter
    def main_dict(self, value: dict):
        assert type(value) is dict
        self._dict = value
        self._is_updated = True

    @property
    def table(self):
        return self._table

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value
        self._is_updated = True

    def __delitem__(self, key):
        del self._dict[key]
        self._is_updated = True


class SessionManager:
    """ Хранит коллекцию связанных объектов DataItem """

    def __init__(self, context_store: ContextStorage, dict_id: dict):
        """
        :param context_store:
        :param dict_id:
        """
        self._context_store = context_store
        self.main_dict_id = dict_id
        self.context_item: ContextItem = self._context_store.get_context(dict_id)
        self._remote_context_sending_list = list()

        # Проводим все изменения таблиц, которые были отправлены удаленными контекстами
        del_set = set()
        for di_name, inst_list in self.context_item.remote_instructions.items():
            try:
                cur_di: DataItem = self.context_item.session[di_name]
            except KeyError:
                pass
            else:
                del_set.add(di_name)
                for type_operation, value in inst_list:
                    if type_operation == 'ins':
                        cur_di.ins_upd_in_table(value)
                    elif type_operation == 'del':
                        cur_di.del_in_table(value)
        for di_name in del_set:
            del self.context_item.remote_instructions[di_name]

    def set_to_store(self):
        self._context_store.set_context(
            self.main_dict_id,
            self.context_item
        )
        for dict_id, di_name, value_list in self._remote_context_sending_list:
            self._context_store.send_remote_instructions(
                dict_id=dict_id,
                di_name=di_name,
                instr_list=value_list
            )
        self._remote_context_sending_list.clear()

    def unlock_store(self):
        self._context_store.set_context(
            self.main_dict_id
        )

    def send_to_remote_context(self, dict_id: dict, di_name: str, value_list: list):
        """ Отрасляем данные в удаленный контекст """
        if dict_id != self.main_dict_id:
            self._remote_context_sending_list.append((dict_id, di_name, value_list))

    def _requrce(self, di: DataItem, del_set: set):
        for v in di.children_set:
            try:
                del_set = self._requrce(self.context_item.session[v], del_set)
            except KeyError:
                pass
        del_set = del_set | di.children_set
        return del_set

    def _get_del_set(self, di_name):
        del_set = set()
        try:
            di_ = self.context_item.session[di_name]
        except KeyError:
            return del_set

        return self._requrce(di_, del_set)

    def is_di_in(self, name):
        return name in self.context_item.session

    def get_data_item(self, name, parent_name=None, is_permanent=False, key_set=None):
        """ возвращает элемент по его имени и имени родителя, если такого элемента нет,
        то предварительно создает его, а затем возвращает """
        if parent_name == name:
            parent_name = None
        # Прежде  всего проверяем существование родителя
        if parent_name is not None:
            try:
                self.context_item.session[parent_name]
            except KeyError:
                raise UnknownParentError('Parent name "{}" is empty'.format(parent_name))

        # Работаем с основныи DataItem
        try:
            cur_di: DataItem = self.context_item.session[name]
        except KeyError:
            result = DataItem(name=name, parent=parent_name, is_permanent=is_permanent, key_set=key_set)
        else:
            if name is not None and cur_di.parent == parent_name:
                result: DataItem = deepcopy(cur_di)
                result.is_permanent = is_permanent
                result.children_set.clear()
            else:
                result = DataItem(name=name, parent=parent_name, is_permanent=is_permanent, key_set=key_set)

        return result

    @staticmethod
    def _add_alias_dict(a_dict: dict, a_set: set, di_name):
        """
        Осуществляет добление в словарь алиасов элементов на основе множества
        """
        if di_name is not None:
            for v in a_set:
                try:
                    cur_item: set = a_dict[v]
                except KeyError:
                    cur_item = set()
                cur_item.add(di_name)
                a_dict[v] = cur_item
        return a_dict

    def _drop_child_branch_di(self, di: DataItem,
                              del_aliases_dict: dict,
                              upd_aliases_dict: dict) -> (dict, dict):
        """
            Удаляет элемент и дочернюю ветку
        """
        # last_di = None
        if di.name is None:
            return upd_aliases_dict, del_aliases_dict

        # Получаем список элементов на удаление
        del_set = self._get_del_set(di.name)
        if di.parent in del_set:
            raise DeletedParentError('Parent of the DataItem ("{}") will be deleted'.format(di.parent))

        # Удаляем дочерние элементы
        for v in del_set:
            try:
                cur_di: DataItem = self.context_item.session[v]
            except KeyError:
                pass
            else:
                del self.context_item.session[v]
                del cur_di

            # Удаляем множества алиасов соответствующие удаленным элементам DataItem
            try:
                cur_alias_set: set = self.context_item.corr_aliases[v]
            except KeyError:
                pass
            else:
                del self.context_item.corr_aliases[v]
                del_aliases_dict = self._add_alias_dict(del_aliases_dict, cur_alias_set, v)

        # Работаем с основным элементов
        try:
            cur_di = self.context_item.session[di.name]
        except KeyError:
            # Такого di еще нет, и не нужно ничего удалять и обновлять
            # Находим родителя
            if di.parent is not None:
                try:
                    par_di: DataItem = self.context_item.session[di.parent]
                except KeyError:
                    di.parent = None
                else:
                    temp_set = par_di.children_set
                    temp_set.add(di.name)
                    par_di.children_set = temp_set
        else:
            # Такой DataItem уже есть и нужно проверить различаются ли родительские элементы
            if cur_di.parent != di.parent:
                # Родители НЕ совпадают, поэтому элемент встраивается в новое место
                # Удаляем из множества дочерних элементов у прежнего родителя
                try:
                    par_cur_di: DataItem = self.context_item.session[cur_di.parent]
                except KeyError:
                    cur_di.parent = None
                else:
                    temp_set = par_cur_di.children_set
                    temp_set.remove(cur_di.name)
                    par_cur_di.children_set = temp_set

                # Добавляем в множество дочерних элементов у нового родителя
                try:
                    par_di: DataItem = self.context_item.session[di.parent]
                except KeyError:
                    di.parent = None
                else:
                    temp_set = par_di.children_set
                    temp_set.add(di.name)
                    par_di.children_set = temp_set

                # формируем множество алиасов удаленных сообщений
                try:
                    cur_alias_set: set = self.context_item.corr_aliases[di.name]
                except KeyError:
                    pass
                else:
                    del self.context_item.corr_aliases[di.name]
                    del_aliases_dict = self._add_alias_dict(del_aliases_dict, cur_alias_set, di.name)
            else:
                # формируем множество обновленных алиасов
                try:
                    cur_alias_set: set = self.context_item.corr_aliases[di.name]
                except KeyError:
                    pass
                else:
                    # del self.corr_aliases[di.name]
                    upd_aliases_dict = self._add_alias_dict(upd_aliases_dict, cur_alias_set, di.name)
            # last_di = cur_di

        return upd_aliases_dict, del_aliases_dict

    def di_embedding(self, di: DataItem) -> (dict, dict):
        """
        Встраиваем элемент DataItem в иерархию session
        Метод возвращает встраиваемый элемент и список алиасов сообщений, затронутых изменением
        """
        del_aliases_dict = dict()
        upd_aliases_dict = dict()
        upd_aliases_dict, del_aliases_dict = self._drop_child_branch_di(
            di=di,
            del_aliases_dict=del_aliases_dict,
            upd_aliases_dict=upd_aliases_dict
        )
        self.context_item.session[di.name] = di
        return upd_aliases_dict, del_aliases_dict

    def del_not_permanent(self) -> dict:
        """ Удаляет все непостоянные элементы di и их дочерние элементы,
        возвращает словарь алиасов, соответствующих удаляемым элементам
        dict(<alias_1>=set(data_item_1, data_item_2))
        """
        # Удаляем элементы
        del_aliases_dict = dict()
        upd_aliases_dict = dict()
        while True:
            cur_di_name, cur_di = None, None
            for k, v in self.context_item.session.items():
                if not v.is_permanent:
                    cur_di = v
                    cur_di_name = k
                    break
            if cur_di is None:
                break
            # удаляем дочерние элементы
            upd_aliases_dict, del_aliases_dict = self._drop_child_branch_di(
                di=cur_di,
                del_aliases_dict=del_aliases_dict,
                upd_aliases_dict=upd_aliases_dict
            )
            # удаляем запись о текущем элементе у родителей
            if cur_di.parent is not None:
                try:
                    par_cur_di: DataItem = self.context_item.session[cur_di.parent]
                except KeyError:
                    cur_di.parent = None
                else:
                    temp_set = par_cur_di.children_set
                    temp_set.remove(cur_di.name)
                    par_cur_di.children_set = temp_set

            # Удаляем сам элемент
            try:
                del self.context_item.session[cur_di_name]
            except KeyError:
                ...

            # переносим из upd_aliases_dict в del_aliases_dict
            for k, v in upd_aliases_dict.items():
                try:
                    cur_alias_set = del_aliases_dict[k]
                except KeyError:
                    del_aliases_dict[k] = v
                else:
                    del_aliases_dict[k] = cur_alias_set | v
            upd_aliases_dict.clear()
        return del_aliases_dict

    def get_value_hierarchy(self, di: DataItem, key):
        cur_di = di
        # Поднимаемся по иерархии вверх в цикле
        while cur_di is not None:
            try:
                return cur_di[key]
            except KeyError:
                cur_di = self.context_item.session.get(cur_di.parent, None)
        return None

    def get_key_hierarchy(self, di: DataItem):
        """ Возвращает упорядоченный словарь всех ключевых значений для таблицы для всей иерархии DataItem """
        result = OrderedDict()
        cur_di = di
        is_first = True
        while cur_di is not None:
            if cur_di.name is not None or is_first:
                result[cur_di.name] = cur_di.get_current_key()
            cur_di = self.context_item.session.get(cur_di.parent, None)
            is_first = False
        return result

    def get_table_hierarchy(self, di: DataItem):
        """ Возвращает упорядоченный словарь всех таблиц для всей иерархии DataItem """
        result = OrderedDict()
        cur_di = self.context_item.session.get(di.parent, None)
        while cur_di is not None:
            if cur_di.name is not None:
                result[cur_di.name] = cur_di.table
            cur_di = self.context_item.session.get(cur_di.parent, None)
        return result

    # Глобальные методы доступа к произвольному DataItem в сессии

    def get_global_table(self, di_name: str) -> list:
        """ Возвращает таблицу заданного DataItem """
        try:
            di: DataItem = self.context_item.session[di_name]
        except KeyError:
            return list()
        else:
            return list(di.table.values())

    def get_global_main_dict(self, di_name: str) -> dict:
        """ Возвращает словарь заданного DataItem """
        try:
            di: DataItem = self.context_item.session[di_name]
        except KeyError:
            return dict()
        else:
            return di.main_dict


class SessionManagerCollection:
    """ Обертка для SessionManager, создает и хранит объекты SessionManager для соответствующих dict_id """

    def __init__(self, main_dict_id, context_store):
        self._main_dict_id = main_dict_id
        self._main_dict_id_key = get_key(main_dict_id)
        self._collection = dict()
        self._context_store = context_store
        self._anything_locked = False

    def put_cur_data_item_list(self, alias_msg=None):
        # if alias_msg is not None:
        #     for v in self._collection.values():
        #         v[1].put_cur_data_item_list(alias_msg)
        # TODO: Добавить заполнение session.corr_aliases
        ...

    def get_session(self):
        self._anything_locked = True
        dict_id = self._main_dict_id
        key = self._main_dict_id_key

        try:
            dict_id_, res = self._collection[key]
        except KeyError:
            res = SessionManager(context_store=self._context_store, dict_id=dict_id)
            self._collection[key] = (dict_id, res)
        return res

    def del_not_permanent(self):  # Очищаем сесиию от всех непостоянных элементов
        for k, v in self._collection.items():
            v[1].del_not_permanent()

    def set_sessions(self):
        for k, v in self._collection.items():
            dict_id, session_manager = v
            session_manager.set_to_store()
        self._collection.clear()
        self._anything_locked = False

    def unlock(self):
        if self._anything_locked:
            for k, v in self._collection.items():
                dict_id, session_manager = v
                session_manager.unlock_store()
            self._anything_locked = False

    def __del__(self):
        self.unlock()


class DataItemConnector(DataItemInterface):
    """ Класс объекта, который создает интерфейс доступа к контексту,
        Повторяет функционал DataItem, но для всей иерархии объектов DataItem
    """

    def __init__(self, session_manager: SessionManager, di_name: str, parent_di_name: str = None,
                 is_permanent=False, dict_id=None, is_changed=False, key_set=None):
        """

        :param session_manager: Сессия
        :param di_name: имя текущего элемента data_item
        :param parent_di_name: имя родительского элемента
        :param is_permanent: Если Data_item перманентный, то значит его не нужно удалять после завершения сессии
                            (под сессией подразумевается непрерывная последовательность выполнения Vertex)
        :param dict_id: в случае если необходимо отправить инфу в несвой контекст, этот момент будет
                            отработан в методе embed()
        :param key_set: Множество ключевых значений
        """
        assert isinstance(session_manager, SessionManager)
        assert type(di_name) is str or di_name is None
        assert type(parent_di_name) is str or parent_di_name is None
        assert type(key_set) is set or key_set is None

        self._session_manager: SessionManager = session_manager
        self._di_name = di_name
        self._parent_di_name = parent_di_name  # может отличаться от self._cur_di.parent, см. DataItem.__init__
        self._cur_di: DataItem = self._session_manager.get_data_item(
            name=di_name,
            parent_name=parent_di_name,
            is_permanent=is_permanent
        )
        if key_set is not None:
            if self._cur_di.key_set == key_set:
                key_set_flag = False
            else:
                self._cur_di.key_set = key_set
                key_set_flag = True
        else:
            key_set_flag = False
        self._update_aliases_dict = dict()
        self._del_aliases_dict = dict()
        self._emd_flag = False
        self._is_changed = key_set_flag or is_changed  # флаг показывает изменен ли элемент DataItem
        self.model_cursor = None  # индекс первого обработанного элемента в таблице
        self._dict_id = dict_id if dict_id is not None else self._session_manager.main_dict_id
        self.change_table_list = list()  # список изменений главной таблицы
        # self._is_update = di_name is not None and di_name == parent_di_name

    @property
    def dict_id(self) -> dict:
        return self._dict_id

    @dict_id.setter
    def dict_id(self, value: dict):
        if not self._emd_flag and type(value) is dict:
            self._dict_id = value

    # Интерфейс для модели
    def ins_upd_in_table(self, value: dict):
        self._cur_di.ins_upd_in_table(value)
        if not self._emd_flag:
            self.change_table_list.append(('ins', value))
        self._is_changed = True

    def del_in_table(self, value):
        self._cur_di.del_in_table(value)
        if not self._emd_flag:
            self.change_table_list.append(('del', value))
        self._is_changed = True

    def clear_table(self):
        if len(self._cur_di.table) > 0:
            self._cur_di.clear_table()
            self._is_changed = True

    def get_cursor_by_value(self, value: dict):
        return self._cur_di.get_cursor_by_value(value)

    # Интерфейс для доступа к main_dict
    @property
    def cursor(self):
        return self._cur_di.cursor

    @cursor.setter
    def cursor(self, value):
        self._cur_di.cursor = value
        self._is_changed = True

    @property
    def main_dict(self):
        return self._cur_di.main_dict

    @main_dict.setter
    def main_dict(self, value: dict):
        self._cur_di.main_dict = value
        self._is_changed = True

    @property
    def is_permanent(self):
        return self._cur_di.is_permanent

    @is_permanent.setter
    def is_permanent(self, value):
        if type(value) is bool:
            self._cur_di.is_permanent = value

    @property
    def table(self):
        return self._cur_di.table

    def __getitem__(self, key):
        return self._session_manager.get_value_hierarchy(self._cur_di, key)

    def __setitem__(self, key, value):
        self._cur_di[key] = value
        self._is_changed = True

    def __delitem__(self, key):
        del self._cur_di[key]
        self._is_changed = True

    @property
    def key_hierarchy(self):
        return self._session_manager.get_key_hierarchy(self._cur_di)

    @property
    def table_hierarchy(self):
        return self._session_manager.get_table_hierarchy(self._cur_di)

    @property
    def key_set(self):
        return copy(self._cur_di.key_set)

    @key_set.setter
    def key_set(self, value):
        if type(value) is set and len(self.table) == 0:
            self._cur_di.key_set = value

    def embed(self):
        if not self._emd_flag:
            if self._cur_di.name is not None:
                if self._is_changed:
                    if self._dict_id == self._session_manager.main_dict_id:
                        self._update_aliases_dict, self._del_aliases_dict = self._session_manager.di_embedding(
                            self._cur_di)
                        self._emd_flag = True
                        self._cur_di = deepcopy(self._cur_di)
                    else:
                        self._session_manager.send_to_remote_context(
                            dict_id=self._dict_id,
                            di_name=self._cur_di.name,
                            value_list=self.change_table_list
                        )
                # else:
                #     # Если никаких изменений в элементе не было и при этом элемент только что был создан
                #     # необходимо вставить элемент в session_manager, это можно сделать с помощью
                #     # того же метода di_embedding
                #     # try:
                #     #     temp = self._session_manager[self._cur_di.name]
                #     # except KeyError:
                #     #     last_di, self._del_list = self._session_manager.di_embedding(self._cur_di)
                #     if not self._session_manager.is_di_in(self._cur_di.name):
                #         self._update_aliases_set, self._del_aliases_set = self._session_manager.di_embedding(
                #             self._cur_di)

                # self._cur_di = deepcopy(self._cur_di)

    @property
    def is_embedding(self):
        return self._emd_flag

    # Глобальные методы доступа к произвольному DataItem в сессии

    def get_global_table(self, di_name: str) -> list:
        """ Возвращает таблицу заданного DataItem """
        return self._session_manager.get_global_table(di_name)

    def get_global_main_dict(self, di_name: str) -> dict:
        """ Возвращает главный словарь заданного DataItem """
        return self._session_manager.get_global_main_dict(di_name)


class DataItemConnectorBuilder:
    _is_permanent = False

    def __init__(self, name, parent, is_permanent=False, dict_id=None, key_set=None):
        """
        :param name: Имя текущего DataItem
        :param parent: имя родительского DataItem
        :param is_permanent: флаг показывает нужно ли удалять текущий DataItem
            и все его дочерние элементы в конце обработки
        :param dict_id: если планируется обращение к глобальному контексту,
            то в dict_id указывается идентификато этого элемента, клиент с dict_id, соответствующим
            глобальному контексту не сможет работать только в своем контексте, это сделано, чтоб
            предотвратить deadlock
        :param key_set: множество ключевых полей
        """
        assert type(name) is str or name is None
        assert type(parent) is str or parent is None
        assert type(is_permanent) is bool
        assert type(dict_id) is dict or dict_id is None
        assert key_set is None or type(key_set) is set

        self.di_name = name
        self._parent = parent
        self._is_permanent = is_permanent
        self.dict_id = dict_id
        self.key_set = key_set
        # t = get_key(dict_id)
        # if t in self.global_set:
        #     raise SessionManagerCollectionError('')

    def get_name(self):
        return self.di_name

    def __call__(self, session_manager: SessionManager):
        assert isinstance(session_manager, SessionManager)

        return DataItemConnector(session_manager=session_manager,
                                 di_name=self.di_name,
                                 parent_di_name=self._parent,
                                 is_permanent=self._is_permanent,
                                 dict_id=self.dict_id,
                                 key_set=self.key_set)
