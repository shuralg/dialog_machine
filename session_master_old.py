#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

""" Классы для обеспечения сессии.
 Сессии организованы более сложным образом чем просто словарь, в который сваливается всё подряд.
 В реализации сделана попытка структурировать их и сделать привязку к структуре используемой модели,
 это задумано с целью обеспечения целостности данных, хранимых в сессии"""

# import logging
from copy import copy, deepcopy
from dialog_machine.project_exceptions import *
from pprint import pformat
import hashlib


def get_key(value):
    """ Находит hash любого объекта python, необходимо для короткого ключа redis """
    # return str(value['chat_id']) + str(value['user_id'])
    return hashlib.md5(pformat(value).encode()).hexdigest()


def key_eq(req_key, list_key):
    """ Проверяем равны ли ключевые словари """
    # if req_key is not None:
    for k, v in req_key.items():
        try:
            if v != list_key[k]:
                return False
        except KeyError:
            return False
    return True


class DataItem:
    """ Единица данных в сессии"""
    _is_global = False  # Если True то элемент не удаляется после окончания сессии

    def __init__(self, name, parent=None, children_set=None, di_dict=None, di_list=None,
                 cursor=None, cur_key_list=None, is_global=False):
        """
        name - имя DataItem
        parent - имя родительского DataItem
        children_set - множество имен дочерних DataItem
        di_dict - установка словаря
        di_list - установка внутреннего списка
        cursor - номер элемента в списке di_list, соответствующего di_dict
        cur_key_list - ключ, указывающий на элемен модели, соответствующий заданному курсору
        """
        assert isinstance(name, str) or name is None
        assert isinstance(parent, str) or parent is None
        assert isinstance(children_set, set) or children_set is None

        self._name = name
        self._dict = di_dict if di_dict is not None and isinstance(di_dict, dict) else dict()
        self._list = di_list if di_list is not None and isinstance(di_list, list) else list()
        if type(cursor) is int:
            self._cursor = cursor if len(self._list) >= cursor > 0 else 1
        else:
            self._cursor = None
        self._cur_key_list = cur_key_list
        if name == parent:
            self._parent = None
        else:
            self._parent = parent
        self._children_set = children_set if children_set is not None else set()
        self._is_global = is_global

    @property
    def name(self):
        return self._name

    @property
    def is_global(self):
        return self._is_global

    @is_global.setter
    def is_global(self, value):
        if type(value) is bool:
            self._is_global = value

    def __getitem__(self, key):
        if key is None:
            return self.get_list()
        elif key == '_cursor':
            return self.cursor
        elif key == '_len':
            return len(self._list)
        else:
            return self._dict[key]

    def __setitem__(self, key, value):
        if key is None:
            self.update_list(value)
        elif key == '_cursor':
            self.cursor = value
        elif key == '_len':
            pass
        else:
            self._dict[key] = value

    def __delitem__(self, key):
        if key is None:
            raise KeyError('key = None')
        elif key == '_cursor':
            raise KeyError('key = "_cursor"')
        elif key == '_len':
            raise KeyError('key = "_len"')
        else:
            del self._dict[key]

    def get_value(self, key):
        if key == '_cursor':
            return self.cursor
        return self._dict[key]

    def set_value(self, key, value):
        if key == '_cursor':
            self.cursor = value
        else:
            self._dict[key] = value

    def set_dict(self, a_dict: dict):
        assert isinstance(a_dict, dict)
        a_dict_ = dict()
        a_dict_ = deepcopy(a_dict)
        try:
            cursor = a_dict_['_cursor']
        except KeyError:
            pass
        else:
            self.cursor = cursor
            del a_dict_['_cursor']
        try:
            del a_dict_['_len']
        except KeyError:
            pass
        self._dict = a_dict_

    def get_dict(self):
        d = deepcopy(self._dict)
        # d['_cursor'] = self._cursor
        # d['_len'] = len(self._list)
        return d

    def get_list(self):
        return self._list

    def clear_list(self):
        self._list.clear()

    def clear_dict(self):
        self._dict.clear()

    def update_list(self, upd_list):
        """ Обновляем главный список исходя из списка обновления """
        assert isinstance(upd_list, list)
        f_di_i = None
        flag_none_return_el = True

        del_list, u_list = list(), list()
        for k, v in upd_list:
            del_list.clear()
            u_list.clear()

            flag_1 = False
            for i, (kk, vv) in enumerate(self._list):
                # Сравниваем k и kk
                if key_eq(k, kk):
                    flag_1 = True
                    # Элемент соответствует ключу, переходим к его модификации (удалению/изменению)
                    if v is None:
                        # удаляем строку
                        del_list.append(i)
                    else:
                        # заменяем строку
                        u_list.append((i, (kk, v)))
                        if flag_none_return_el:
                            f_di_i, flag_none_return_el = i, False
            if not flag_1:
                # Ни одного соответствия ключей не было найдено, значит делаем вывод, что
                # элемент предназначен для вставки, если конечно v не None
                if v is not None:
                    self._list.append((k, v))
                    if flag_none_return_el:
                        f_di_i, flag_none_return_el = len(self._list) - 1, False
            else:
                # Сначала обновляем элементы списка
                for update_el, update_value in u_list:
                    self._list[update_el] = update_value

                if flag_none_return_el:
                    try:
                        f_di_i = del_list[0]
                    except IndexError:
                        f_di_i = None
                    else:
                        flag_none_return_el = False
                f_di_i = f_di_i - sum(1 for v in del_list if v < f_di_i)
                # else:
                # Удаляем элементы списка
                del_list.sort(reverse=True)
                for del_i in del_list:
                    self._list.pop(del_i)

        # Находим возвращаемое значение
        len_list = len(self._list)
        return_key = None
        if len_list > 0 and f_di_i is not None:
            if f_di_i < 0:
                return_key = self._list[0][0]
            elif f_di_i < len_list:
                return_key = self._list[f_di_i][0]
            else:
                return_key = self._list[len_list - 1][0]

        return return_key

    def accordance_cur_key_list(self):
        """ Метод приводит в соответствие cur_key_list курсору """
        if type(self._cur_key_list) is not dict:
            self._cur_key_list = None
        else:
            self._set_cursor(self._cursor)
            el = self._list[self._cursor - 1]
            try:
                temp = el[0]
            except IndexError:
                raise FormatDataItemListError("Format of list is DataItem {} error".format(self._name))
            # assert isinstance(temp, dict)
            self._cur_key_list = temp
        return self._cur_key_list

    def accordance_cursor(self):
        """ Метод приводит cursor в соответствие cur_key_list """
        if self._cur_key_list is not None:
            for i, el in enumerate(self._list, 1):
                try:
                    temp = el[0]
                except IndexError:
                    raise FormatDataItemListError("Format of list is DataItem {} error".format(self._name))
                # assert isinstance(temp, dict)
                if key_eq(self._cur_key_list, temp):
                    self._cur_key_list = temp
                    self._cursor = i
                    return
        self._cur_key_list = None
        self._cursor = None
        return self._cursor

    def get_cur_data_list(self):
        """ Возвращает раздел данных (словарь) элемента главного списка по курсору """
        if self._cursor is not None:
            if type(self._cursor) is int:
                self._set_cursor(self._cursor)
            else:
                return None
            k, data = self._list[self._cursor - 1]
            return data
        else:
            return None

    @property
    def cursor(self):
        return self._cursor

    def _set_cursor(self, value):
        l = len(self._list)
        if l > 0:
            if l >= value > 0:
                self._cursor = value
            elif value > l:
                self._cursor = l
            else:
                self._cursor = 1
        else:
            self._cursor = None

    @cursor.setter
    def cursor(self, value):
        # assert type(value) is int or value is None
        if type(value) is not int:
            self._cursor = None
        else:
            self._set_cursor(value)

    @property
    def cur_key_list(self):
        return self._cur_key_list

    @cur_key_list.setter
    def cur_key_list(self, value):
        assert isinstance(value, dict) or value is None
        self._cur_key_list = value

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        assert isinstance(value, str) or value is None
        self._parent = value

    @property
    def children(self):
        return self._children_set

    @children.setter
    def children(self, value):
        assert isinstance(value, set)
        self._children_set = value


class SessionManager:
    """ Хранит коллекцию связанных объектов DataItem """

    def __init__(self, session=None):
        # assert isinstance(session, dict) or session is None
        if type(session) is tuple:
            self._main_session = session
        elif type(session) is dict:
            self._main_session = (dict(), session)
        else:
            self._main_session = (dict(), dict())
        self._session = self._main_session[1]
        self._corr_dict = self._main_session[0]  # Соответствия dict_item определенному набору алиасов сообщений
        self._cur_data_item_list = list()

    # <Обработка соответствия dataItem и aliases_msg>

    def add2cur_data_item_list(self, data_item: DataItem):
        if data_item.is_global:
            self._cur_data_item_list.append(data_item.name)

    def put_cur_data_item_list(self, alias_msg=None):
        if alias_msg is not None:
            for v in self._cur_data_item_list:
                temp_set = self._corr_dict.get(v, set())
                temp_set.add(alias_msg)
                self._corr_dict[v] = temp_set
        self._cur_data_item_list.clear()

    def get_aliases(self, list_of_di_name: list):
        """ Получаем список алиасов сообщений, соответствующих DataItem-ам,
        имена которых указаны во входном списке, соответствующие ключи удаляем в _corr_dict """
        aliases_set = set()
        for v in list_of_di_name:
            try:
                aliases_set = aliases_set | set(self._corr_dict[v])
            except:
                continue
            else:
                del self._corr_dict[v]
        return list(aliases_set)

    # </Обработка соответствия dataItem и aliases_msg>

    def get_session(self):
        return self._main_session

    def __getitem__(self, key):
        """ Возвращает копию словаря DataItem """
        temp = self._session[key]
        return temp.get_dict()

    def is_di_in(self, name):
        return name in self._session

    def get_di_by_name(self, name, parent_name=None, is_global=False):
        """ возвращает элемент по его имени и имени родителя, если такого элемента нет, то создает его """
        # Прежде  всего проверяем существование родителя
        if parent_name is not None and name != parent_name:
            try:
                parent_di = self._session[parent_name]
            except KeyError:
                raise UnknownParentError('Parent name "{}" is empty'.format(parent_name))
        else:
            parent_di = None

        # Работаем с основныи DataItem
        try:
            cur_di = self._session[name]
        except KeyError:
            if parent_name != name:
                result = DataItem(name=name, parent=parent_name, is_global=is_global)
            else:
                result = DataItem(name=name, parent=None, is_global=is_global)
            # Если такого элемента не было, но добавляем его в сессию
            # Потому что хоть он и пустой, но он может не измениться и cоответственно не быть записан,
            # но впоследствии у него могут быть созданы дочерние элементы
            # self._session[name] = result
            # if parent_di is not None:
            #     parent_di.children.add(name)
        else:
            if parent_name == name and name is not None:
                result: DataItem = deepcopy(cur_di)
                result.is_global = is_global
                result.children = set()
            else:
                result = DataItem(name=name, parent=parent_name, is_global=is_global)

        return result

    def get_value_hierarchy(self, di, key):
        cur_di = di
        # Поднимаемся по иерархии вверх в цикле
        while cur_di is not None:
            try:
                return cur_di.get_value(key)
            except KeyError:
                cur_di = self._session.get(cur_di.parent, None)
        return None

    def get_key_dict_hierarchy(self, di):
        """ Возвращает словарь всех ключевых значений для таблицы для всей иерархии DataItem """
        result = dict()
        cur_di = di
        while cur_di is not None:
            if cur_di.name is not None:
                result[cur_di.name] = copy(cur_di.cur_key_list)
            cur_di = self._session.get(cur_di.parent, None)
        return result

    def get_parent_hierarchy(self, di):
        """ Возвращает иерархию имен всех элементов DataItem """
        result = list()
        cur_di = di
        while cur_di is not None:
            if cur_di.name is not None:
                result.append(cur_di.name)
            else:
                break
            cur_di = self._session.get(cur_di.parent, None)
        return result

    def _get_del_set_old(self, di_name):
        del_set = set()
        try:
            di_ = cur_di = self._session[di_name]
        except KeyError:
            return del_set
        while True:
            temp_set = cur_di.children.difference(del_set)
            if di_.name == cur_di.name and len(temp_set) == 0:
                # del_set.add(cur_di.name)
                break
            try:
                next_di_name = list(temp_set)[0]
            except IndexError:
                del_set.add(cur_di.name)
                if cur_di != di_:
                    cur_di = self._session.get(cur_di.parent, di_)
            else:
                cur_di = self._session.get(next_di_name, di_)
        return del_set

    def _requrce(self, di: DataItem, del_set: set):
        di_list = list(di.children)
        for v in di_list:
            try:
                del_set = self._requrce(self._session[v], del_set)
            except KeyError:
                pass
        del_set = del_set | di.children
        return del_set

    def _get_del_set(self, di_name):
        del_set = set()
        try:
            di_ = cur_di = self._session[di_name]
        except KeyError:
            return del_set

        return self._requrce(di_, del_set)

    def di_embedding(self, di):
        """ Встраиваем элемент DataItem в иерархию session """
        if di.name is None:
            return None, list()
        # Удаляем элементы
        del_set = self._get_del_set(di.name)
        if di.parent in del_set:
            raise DeletedParentError('Parent of the DataItem ("{}") will be deleted'.format(di.parent))
        del_list = list(del_set)
        del_glob_list = list()  # список удаленных глобальных элементов
        for v in del_list:
            try:
                cur_di: DataItem = self._session[v]
            except KeyError:
                pass
            else:
                if cur_di.is_global:
                    del_glob_list.append(v)
                del self._session[v]
                del cur_di

        last_di = None
        try:
            cur_di = self._session[di.name]
        except KeyError:
            # Такого di еще нет, и не нужно ничего удалять и обновлять
            # Находим родителя
            if di.parent is not None:
                try:
                    par_di = self._session[di.parent]
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
                    par_cur_di = self._session[cur_di.parent]
                except KeyError:
                    cur_di.parent = None
                else:
                    temp_set = par_cur_di.children_set
                    temp_set.remove(cur_di.name)
                    par_cur_di.children_set = temp_set

                # Добавляем в множество дочерних элементов у нового родителя
                try:
                    par_di = self._session[di.parent]
                except KeyError:
                    di.parent = None
                else:
                    temp_set = par_di.children_set
                    temp_set.add(di.name)
                    par_di.children_set = temp_set
            last_di = cur_di

        self._session[di.name] = di
        return last_di, del_glob_list

    def del_not_global(self):
        """ Удаляет все неглобальне элементы di и их дочерние элементы """
        # Удаляем элементы
        del_set = set()
        for k, v in self._session.items():
            if k not in del_set and not v.is_global:
                del_set = del_set | self._get_del_set(k)
                del_set.add(k)
        for v in list(del_set):
            try:
                cur_di: DataItem = self._session[v]
            except KeyError:
                pass
            else:
                del self._session[v]
                del cur_di
        for v in self._session.values():
            v.children = v.children - del_set


class SessionManagerCollection:
    """ Обертка для SessionManager, создает и хранит объекты SessionManager для соответствующих dict_id """

    def __init__(self, main_dict_id, context_store):
        self._main_dict_id = main_dict_id
        self._main_dict_id_key = get_key(main_dict_id)
        self._collection = dict()
        self._context_store = context_store
        self._anything_locked = False

    def put_cur_data_item_list(self, alias_msg=None):
        if alias_msg is not None:
            for v in self._collection.values():
                v[1].put_cur_data_item_list(alias_msg)

    def get_session(self, dict_id, global_set):
        key = get_key(dict_id)
        if key != self._main_dict_id_key and self._main_dict_id_key in global_set:
            raise SessionManagerCollectionError(
                f'Context for "{self._main_dict_id}" is global contex? and cannot use other contexts')
        try:
            dict_id_, res = self._collection[key]
        except KeyError:
            t = self._context_store.get_context(dict_id)
            var_state = t.get('var_state', dict())
            res = SessionManager(var_state)
            self._collection[key] = (dict_id, res)
        self._anything_locked = True
        return res

    def del_not_global(self):  # Очищаем сесиию от всех неглобальных элементов
        for k, v in self._collection.items():
            v[1].del_not_global()

    def set_sessions(self):
        for k, v in self._collection.items():
            dict_id, session_manager = v
            self._context_store.set_context(dict_id, dict(var_state=session_manager.get_session()))
        self._anything_locked = False

    def unlock(self):
        if self._anything_locked:
            for k, v in self._collection.items():
                dict_id, session_manager = v
                self._context_store.set_context(dict_id)
            self._anything_locked = False

    def __del__(self):
        self.unlock()


class DataItemConnector:
    """ Класс объекта, который создает интерфейс доступа к Сессии """

    def __init__(self, session_manager: SessionManager, di_name: str, parent_di_name: str = None,
                 is_global=False):
        """

        :param session_manager: Сессия
        :param di_name: имя текущего элемента data_item
        :param parent_di_name: имя родительского элемента
        :param is_global: Если Data_item глобальный, то значит его не нужно удалять после завершения сессии
                            (под сессией подразумевается непрерывная последовательность выполнения Vertex)
        """
        assert isinstance(session_manager, SessionManager)
        assert type(di_name) is str or di_name is None
        assert type(parent_di_name) is str or parent_di_name is None

        self._session_manager = session_manager
        self._di_name = di_name
        self._parent_di_name = parent_di_name  # может отличаться от self._cur_di.parent, см. DataItem.__init__
        self._cur_di = self._session_manager.get_di_by_name(name=di_name,
                                                            parent_name=parent_di_name,
                                                            is_global=is_global)
        self._update_list = list()
        self._del_list = list()
        self._emd_flag = False
        self._is_changed = False
        self._is_update = di_name is not None and di_name == parent_di_name

    @property
    def is_global(self):
        return self._cur_di.is_global

    @is_global.setter
    def is_global(self, value):
        self._cur_di.is_global = value

    # методы до выполнения embedding

    def get_list(self):
        return [v[1] for v in self._cur_di.get_list()]

    def get_full_list(self):
        """ В отличие от метода get_list() возвращает список с ключевыми полями """
        return self._cur_di.get_list()

    def perform_model(self, model_list):
        """ Метод, вызываемый при выполнении объекта модели,
            model_list = [(key_dict, data_dict), ... ]

            1) находим cur_key_list, соответствующий курсору (шаг выполняется только при self._is_update == True)
            2) при self._is_update == False полностью сбрасываем главный список
            3) Записываем все изменения в главный список (выполняем модель)
            4) Находим элемент главного списка, соответствующий cur_key_list,
                если такого элемента нет, то берем первый измененный элемент
                    главного списка и корректируем cur_key_list в соответствии с взятым элементом;
                если все изменения главного списка были связанны с удалением строк, то устанавливаем cur_key_list
                    на следующий элемент после первого же измененного элемента
            5) ставим cursor в соответствие с cur_key_list
            6) переносим data_dict элемента главного списка в главный словарь
            """
        assert isinstance(model_list, list)
        model_list_ = deepcopy(model_list)
        if not self._emd_flag:
            if self._is_update:
                # Шаг 1
                self._cur_di.accordance_cur_key_list()
            else:
                # Шаг 2
                self._cur_di.clear_list()

            # Шаг 3
            alter_key_list = self._cur_di.update_list(model_list_)

            # Шаг 4,5
            if self._cur_di.accordance_cursor() is None:
                if alter_key_list is not None:
                    self._cur_di.cur_key_list = alter_key_list
                    self._cur_di.accordance_cursor()
            if self._cur_di.cursor is None:
                self._cur_di.cursor = 1
                self._cur_di.accordance_cur_key_list()

            # Шаг 6
            data_dict = self._cur_di.get_cur_data_list()
            if data_dict is not None:
                self._cur_di.set_dict(data_dict)
            else:
                self._cur_di.set_dict(dict())

            if alter_key_list is not None:
                self._is_changed = True

    def get_dict(self):
        return self._cur_di.get_dict()

    def set_dict(self, value):
        # assert isinstance(value, dict)
        d_ = self.get_dict()
        if d_ != value:
            self._cur_di.set_dict(value)
            self._is_changed = True

    def append_dict(self, value):
        # assert isinstance(value, dict)
        d_ = self.get_dict()
        last_d = copy(d_)
        for k, v in value.items():
            d_[k] = v
        if d_ != last_d:
            self._cur_di.set_dict(d_)
            self._is_changed = True

    @property
    def cursor(self):
        return self._cur_di.cursor

    @cursor.setter
    def cursor(self, value):
        if value != self._cur_di.cursor:
            self._cur_di.cursor = value
            d = self._cur_di.get_cur_data_list()
            if d is not None:
                self._cur_di.set_dict(d)
            else:
                self._cur_di.set_dict(dict())
            self._is_changed = True

    @property
    def name(self):
        return self._cur_di.name

    @property
    def parent(self):
        return self._cur_di.parent

    @property
    def model_keys(self):
        """ Возвращаем все ключевые значения для текущего DataItem, в соответствии
            с иерархией самого элемента """
        return self._session_manager.get_key_dict_hierarchy(self._cur_di)

    def parents_list(self):
        """ Возвращает список иерархии родителей """
        return self._session_manager.get_parent_hierarchy(self._cur_di)

    def embed(self):
        if not self._emd_flag:
            if self._cur_di.name is not None:
                if self._is_changed:
                    last_di, self._del_list = self._session_manager.di_embedding(self._cur_di)
                    if last_di is not None and last_di.is_global:
                        if self._is_update:
                            self._update_list = [self.name]
                        else:
                            self._del_list.append(self.name)
                else:
                    # Если никаких изменений в элементе не было и при этом элемент только что был создан
                    # необходимо вставить элемент в session_manager, это можно сделать с помощью
                    # того же метода di_embedding
                    # try:
                    #     temp = self._session_manager[self._cur_di.name]
                    # except KeyError:
                    #     last_di, self._del_list = self._session_manager.di_embedding(self._cur_di)
                    if self._session_manager.is_di_in(self._cur_di.name) or not self._is_update:
                        last_di, self._del_list = self._session_manager.di_embedding(self._cur_di)

                self._cur_di = deepcopy(self._cur_di)
            self._emd_flag = True

    @property
    def is_embedding(self):
        return self._emd_flag

    # def get_key_dict_hierarchy(self):
    #     result = dict()
    #     cur_di = self._cur_di
    #     while

    def __getitem__(self, key):
        if type(key) is int:
            l_ = self._cur_di.get_list()
            try:
                return l_[key][1]
            except KeyError:
                return None
        return self._session_manager.get_value_hierarchy(self._cur_di, key)

    def __setitem__(self, key, value):
        if key is None and not isinstance(value, list):
            return
        if type(key) is int:
            return
        try:
            cur_value = self._cur_di[key]
        except KeyError:
            self._is_changed = True
        else:
            if cur_value != value:
                self._is_changed = True
        self._cur_di[key] = value

    def __delitem__(self, key):
        if not type(key) is int and key is not None:
            try:
                del self._cur_di[key]
            except KeyError:
                pass
            else:
                self._is_changed = True

    def __len__(self):
        return len(self._cur_di.get_list())

    def __iter__(self):
        try:
            return iter(self.get_list())
        except Exception:
            return iter(list())

    def get_del_upd_lists(self):
        return self._update_list, self._del_list

    def get_del_upd_alias_lists(self):
        alias_del_list = self._session_manager.get_aliases(self._del_list)
        self._del_list.clear()
        alias_upd_list = self._session_manager.get_aliases(self._update_list)
        self._update_list.clear()
        return alias_upd_list, alias_del_list

    def add_cur_id(self):
        self._session_manager.add2cur_data_item_list(data_item=self._cur_di)


class DataItemConnectorBuilder:
    _is_global = False
    global_set = set()  # множество имен всех глобальных контекстов

    def __init__(self, name, parent, is_global=False, dict_id=None):
        """
        :param name: Имя текущего DataItem
        :param parent: имя родительского DataItem
        :param is_global: флаг показывает нужно ли удалять текущий DataItem
            и все его дочерние элементы в конце обработки
        :param dict_id: если планируется обращение к глобальному контексту,
            то в dict_id указывается идентификато этого элемента, клиент с dict_id, соответствующим
            глобальному контексту не сможет работать только в своем контексте, это сделано, чтоб
            предотвратить deadlock
        """
        assert type(name) is str or name is None
        assert type(parent) is str or parent is None
        assert type(is_global) is bool
        assert type(dict_id) is dict or dict_id is None

        self._di_name = name
        self._parent = parent
        self._is_global = is_global
        self.dict_id = dict_id
        # t = get_key(dict_id)
        # if t in self.global_set:
        #     raise SessionManagerCollectionError('')
        if dict_id is not None:
            self.global_set.add(get_key(dict_id))

    def get_name(self):
        return self._di_name

    def __call__(self, session_manager: SessionManager):
        assert isinstance(session_manager, SessionManager)
        return DataItemConnector(session_manager, self._di_name, self._parent)
