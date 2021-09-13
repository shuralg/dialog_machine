#!/usr/bin/python3.7
# -*- coding: utf-8 -*-

import psycopg2
from copy import deepcopy
from dialog_machine.dialog_machine_core import ModelConnectorBase, ModelMasterAbstract, MediaMasterAbstract
from dialog_machine.project_exceptions import DialogMachineModelError


def modify_res(res, cur):
    """ Преобразует список-результат запроса в свисок словарей"""
    result = list()
    for ll in res:
        temp_dict = dict()
        for k, v in zip(cur.description, ll):
            temp_dict[k[0]] = bytes(v) if type(v) is memoryview else v
        result.append(temp_dict)
    return result


class ModelConnectorPostgre(ModelConnectorBase):
    """ класс соединения к хранилищу модели (это не обязательно база данных - это может быть
        какой-нибудь объект c доступом через API)  """

    def __init__(self, database, user, password, host, port):
        super().__init__()
        self._database = database
        self._user = user
        self._password = password
        self._host = host
        self._port = port

    def create_connection(self):
        """ Создает соединение с базой данных и возвращает его """
        return psycopg2.connect(database=self._database,
                                user=self._user,
                                password=self._password,
                                host=self._host,
                                port=self._port)

    def get_id(self):
        """ Возвращает уникальный идентификатор конеретного объекта соединения
            (неизменяемый тип, который можно использовать в качестве ключа элемента словаря)"""
        return self._id

    def execute(self, conn, master_arg, arg):
        """ Выполняет запрос к базе
            conn - объект соединения с моделью, созданный в методе create_connection
                        и переданный обратно внешним окружением
            master_arg - запрос к базе
            arg - список либо словарь аргументов запроса, в зависимости от самого запроса)
            """
        cur = conn.cursor()
        request = master_arg
        if request is None:
            raise DialogMachineModelError('request is None')
        if (isinstance(arg, dict) or isinstance(arg, list)) and len(arg) > 0:
            cur.execute(request, arg)
        else:
            cur.execute(request)

        try:
            t = cur.fetchall()
        except psycopg2.ProgrammingError as e:
            return list()
        else:
            return modify_res(t, cur)

    def rollback_model(self, conn, init_dict, arg):
        """ Выполняет запрос к базе (функционал метода из model_master, так сделано для
                локализации методов обращения к базе в одном классе)
            conn - объект соединения с моделью, созданный в методе create_connection
                        и переданный обратно в внешним окружением
            init_dict - словарь с запросами dict(reqest=..., rollback_reqest=...
            arg - аргументы запроса (last_data))
            """
        return

    def close_connection(self, conn):
        """ Закрываем соединение соединение с базой данных и возвращает его
        :param conn:
        """
        conn.close()

    def commit(self, conn):
        """ коммитим запрос в базу
        :param conn:
        """
        conn.commit()

    def rollback(self, conn):
        """ Откатывает транзакцию в случае возникновения ошибок
            conn - объект соединения с моделью, созданный в методе create_connection
                        и переданный обратно в внешним окружением"""
        conn.rollback()


class ModelMasterPostgre(ModelMasterAbstract):
    """ Класс модели """

    def __init__(self, model_connector: ModelConnectorPostgre, key_set, **args):
        """ init_dict - содержит словарь данных этапа инициализации (например запрос к БД)
            model_connector - объект, определяющий физическое взаимодействие с моделью (запросы к базе данных)
            key_set - множество ключевых аргументов

            Предполагается использовать исключения PhysicalModelError и LogicalModelError.
            LogicalModelError - особенно важно, так как на это исключение завязана обработка
             в классе Vertex, оно должно выбрасываться в случае логических проблем в запросе модели
             (например данные не могут быть вставлены или обновлены по какой-то причине и т.д.)
        """

        super().__init__(**args)
        self._model_connector = model_connector
        self._key_set = key_set

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

    # def get_id(self):
    #     """ Метод возвращает уникальный идентификатор соединения """
    #     conn_id = self._model_connector.get_id()
    #     return conn_id

    def _postgre_execute(self, cur_data_item, request, arg, conn, is_ins_upd=True, execute_fun=None):
        """
        Метод выполняет вставку/изменение либо удаление из DataItem на основе запроса к PostGreSQL
        :param cur_data_item:
        :param request:
        :param arg:
        :param conn:
        :param is_ins_upd:
        :param execute_fun:
        :return: данные для возможного rollback
        """
        result = list()
        try:
            temp = self._model_connector.execute(conn=conn, master_arg=request, arg=arg)
        except Exception as e:
            temp = list()
        if is_ins_upd:
            if execute_fun is None:
                for v in temp:
                    value = deepcopy(arg)
                    if v is not None:
                        for kk, vv in v.items():
                            value[kk] = vv
                    cur_data_item.ins_upd_in_table(value)
                    result.append(value)
            else:
                for v in temp:
                    value = deepcopy(arg)
                    if v is not None:
                        for kk, vv in v.items():
                            value[kk] = vv
                    try:
                        value = execute_fun(value)
                    except:
                        pass
                    cur_data_item.ins_upd_in_table(value)
                    result.append(value)
        else:
            for v in temp:
                value = deepcopy(arg)
                if v is not None:
                    for kk, vv in v.items():
                        value[kk] = vv
                cur_data_item.del_in_table(value)
                result.append(value)

        return result

    def main(self, cur_data_item, additional_args: dict, vertex_context_dict: dict,
             conn, media_master: MediaMasterAbstract):
        """ Готовит и возвращает аргументы для запроса, за выполнение основной логики (execute, commit,
            rollback... будут отвечать отдельные методы)
            conn - объект, отвечающий за физическое выпонение запроса
            media_master - обът работы с медиа

            Возвращает data_for_rollback
        """
        return self._postgre_execute(
            cur_data_item=cur_data_item,
            request="",
            arg=additional_args,
            conn=conn,
            is_ins_upd=True
        )

    def rollback(self, conn, data_for_rollback):
        """ Откат операций над моделью в случае ошибки в последовательности выполнения вершин
            conn - объект, отвечающий за физическое выпонение запроса
            data_for_rollback - аргумент получаемый перед выпонением основного запроса к модели, получаемый методом
                        get_last_data и будет передан по средствам объекта-адаптера
            (Аналогичный метод есть и у ModelConnection, это другой метод,
             вызвается отдельно и содержит разный функционал)
        """
        return
