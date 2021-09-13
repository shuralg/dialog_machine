#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

from dialog_machine.dialog_machine_core import *
import logging

from time import sleep
from random import uniform

import redis
import pickle
from pprint import pformat
import hashlib

from threading import Timer
import uuid


# class RedisStorage:
#     """ Класс объекта для взаимодействия с хранилищем Redis и хранения промежуточной информации
#         обмена, которая разветвляется на ContextStorage и MsgGetter"""
#
#     def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
#         self._redis_host = redis_host
#         self._redis_port = redis_port
#         self._redis_db = redis_db
#         self._temp_dict = dict()
from dialog_machine.interfaces import ContextStorage, MsgGetter, SenderMsg, AbstractSender


def get_hash(value):
    """ Находит hash любого объекта python, необходимо для короткого ключа redis """
    return hashlib.md5(pformat(value).encode()).hexdigest()


class MsgGetterRedisOld(MsgGetterOld):
    """ Класс объекта хранилища данных сообщений и контекста этих сообщений с использованием Redis"""

    # Суффикс для строки названия блокировки геттера
    SUF_STR_GETTER = b"_gv2"

    def __init__(self, common_dict_def, redis_host='localhost', redis_port=6379, redis_db=0,
                 timeout=300, blocking_timeout=310, redis_username=None, redis_password=None):
        """ Если какая-то команда отсутствует, она может быть взята из common_dict_def """
        super().__init__(common_dict_def)
        # self._redis_host = redis_host
        # self._redis_port = redis_port
        # self._redis_db = redis_db
        self._timeout_ = timeout
        self._blocking_timeout = blocking_timeout
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password)

    def get_msg(self, dict_id, msg_id, command):
        token = get_hash(dict_id)
        itemid = str(token).encode()
        lock_name = itemid + self.__class__.SUF_STR_GETTER

        # lock = self._redis.lock(lock_name,
        #                         timeout=self._timeout_,
        #                         blocking_timeout=self._blocking_timeout,
        #                         thread_local=False)
        #
        # with lock.acquire(blocking=True, token=token):
        with self._redis.lock(lock_name,
                              timeout=self._timeout_,
                              blocking_timeout=self._blocking_timeout,
                              thread_local=False):
            # Проверяем не занят ли семафор
            semaphore = self._redis.hget(itemid, 'semaphore')
            if semaphore is not None and semaphore > b"0":
                raise IsMsgProcessError("Message is processing, semaphore={}".format(semaphore))

            temp = self._redis.hget(itemid, "common_dict")
            w_common_dict = pickle.loads(temp) if temp is not None else dict()

            if msg_id is None:
                try:
                    temp_dict = w_common_dict[command]
                except KeyError:
                    try:
                        temp_dict = self._get_control(dict_id, command)
                        # temp_dict = self._common_dict_def[command]

                    except KeyError:
                        raise NoMatchCommandError("Command '{}' is empty".format(command))
                    else:
                        t = deepcopy(temp_dict)
                        res = dict(vertex_name=t.get('vertex_name', None),
                                   control_dict=t.get('control_dict', dict())), \
                              None
                        return res
                msg_id_new = temp_dict.get('msg_id', msg_id)
            else:
                msg_id_new = msg_id
                temp_dict = None  # будет обработано дальше, когда будут считаны данные сообщения

            # Работаем с msg_dict
            temp = self._redis.hget(itemid, "msg_dict")
            w_msg_dict = pickle.loads(temp) if temp is not None else dict()

            # Получаем сообщение
            try:
                temp_msg = w_msg_dict[msg_id_new]
                msg_obj = deepcopy(temp_msg.get('msg_obj', None))
                msg_dict = deepcopy(temp_msg['msg_dict'])
                alias_msg = temp_msg['alias_msg']
            except KeyError:
                raise NoSuchMsgError("No such message had msg_id={}".format(msg_id_new))
            else:
                if temp_dict is None:  # это может быть, если команда была в msg_dict, а не в common_dict
                    try:
                        temp_dict = msg_dict[command]
                    except KeyError:
                        raise NoMatchCommandError("Command '{}' is empty in Msg_dict".format(command))

            # Получаем common_dict
            common_dict = dict()
            for k, v in w_common_dict.items():
                try:
                    if v['msg_id'] == msg_id_new:
                        common_dict[k] = dict(vertex_name=v['vertex_name'],
                                              control_dict=deepcopy(v['control_dict']))
                except KeyError:
                    pass

        t = deepcopy(temp_dict)
        return dict(vertex_name=t.get('vertex_name', None),
                    control_dict=t.get('control_dict', dict())), \
               dict(dict_id=dict_id,
                    msg_id=msg_id_new,
                    msg_obj=msg_obj,
                    common_dict=common_dict,
                    msg_dict=msg_dict,
                    alias_msg=alias_msg)

    def _get_msg_for_alias(self, dict_id, msg_id):
        """ Возвращает сообщение по алиасу, суть в том, что возвращает сообщение минуя механизм семафоров """
        token = get_hash(dict_id)
        itemid = str(token).encode()
        lock_name = itemid + self.__class__.SUF_STR_GETTER

        with self._redis.lock(lock_name,
                              timeout=self._timeout_,
                              blocking_timeout=self._blocking_timeout,
                              thread_local=False):

            temp = self._redis.hget(itemid, "common_dict")
            w_common_dict = pickle.loads(temp) if temp is not None else dict()

            if msg_id is None:
                raise NoSuchMsgError("No such msg by key None")
            else:
                msg_id_new = msg_id
                temp_dict = None  # будет обработано дальше, когда будут считаны данные сообщения

            # Работаем с msg_dict
            temp = self._redis.hget(itemid, "msg_dict")
            w_msg_dict = pickle.loads(temp) if temp is not None else dict()

            # Получаем сообщение
            try:
                temp_msg = w_msg_dict[msg_id_new]
                msg_obj = deepcopy(temp_msg.get('msg_obj', None))
                msg_dict = deepcopy(temp_msg['msg_dict'])
                alias_msg = temp_msg['alias_msg']
            except KeyError:
                raise NoSuchMsgError("No such message had msg_id={}".format(msg_id_new))

            # Получаем common_dict
            common_dict = dict()
            for k, v in w_common_dict.items():
                try:
                    if v['msg_id'] == msg_id_new:
                        common_dict[k] = dict(vertex_name=v['vertex_name'],
                                              control_dict=deepcopy(v['control_dict']))
                except KeyError:
                    pass

        return dict(dict_id=dict_id,
                    msg_id=msg_id_new,
                    msg_obj=msg_obj,
                    common_dict=common_dict,
                    msg_dict=msg_dict,
                    alias_msg=alias_msg)

    def get_id_by_alias(self, alias_msg):
        """ возвращает идентификатор сообщения в системе по алиасу"""
        temp_dict = self._redis.hget("aliases_dict", alias_msg)
        if type(temp_dict) is dict:
            msg_id = temp_dict['msg_id']
            dict_id = temp_dict['dict_id']
            return dict_id, msg_id
        else:
            raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg))

    def get_msg_by_alias(self, alias_msg):
        # self._redis.hset("aliases_dict", alias_msg, pickle.dumps(aliases_dict))
        temp_dict = self._redis.hget("aliases_dict", alias_msg)
        if type(temp_dict) is dict:
            msg_id = temp_dict['msg_id']
            dict_id = temp_dict['dict_id']
            return self._get_msg_for_alias(dict_id, msg_id)
        else:
            raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg))

    def inc_semaphore(self, dict_id):
        """ метод увеличивает семафор, реализует нюнасы работы с redis """
        token = get_hash(dict_id)
        itemid = str(token).encode()
        lock_name = itemid + self.__class__.SUF_STR_GETTER

        # lock = self._redis.lock(lock_name,
        #                         timeout=self._timeout_,
        #                         blocking_timeout=self._blocking_timeout,
        #                         thread_local=False)
        #
        # with lock.acquire(blocking=True, token=token):
        with self._redis.lock(lock_name,
                              timeout=self._timeout_,
                              blocking_timeout=self._blocking_timeout,
                              thread_local=False):
            logging.debug("Инкрементируем семафор")
            self._redis.hincrby(itemid, "semaphore", 1)

    def dec_semaphore(self, dict_id):
        """ метод уменьшения семафора """
        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self.__class__.SUF_STR_GETTER
        with self._redis.lock(lock_name,
                              timeout=self._timeout_,
                              blocking_timeout=self._blocking_timeout,
                              thread_local=False):
            self._redis.hincrby(item_id, "semaphore", -1)

    def set_msg(self, dict_msg_arg):
        """ dict_msg_arg = dict(dict_id=dict_id, msg_id=msg_id_new,
                                msg_obj=msg_obj, common_dict=common_dict, msg_dict=msg_dict) """

        try:
            dict_id = deepcopy(dict_msg_arg['dict_id'])
            msg_id = deepcopy(dict_msg_arg['msg_id'])
            common_dict = deepcopy(dict_msg_arg['common_dict'])
            msg_dict = deepcopy(dict_msg_arg['msg_dict'])
            msg_obj = deepcopy(dict_msg_arg['msg_obj'])
            alias_msg = dict_msg_arg['alias_msg']
        except KeyError:
            return

        if msg_id is None:
            return

        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self.__class__.SUF_STR_GETTER

        # lock = self._redis.lock(lock_name,
        #                         timeout=self._timeout_,
        #                         blocking_timeout=self._blocking_timeout,
        #                         thread_local=False)
        #
        # with lock.acquire(blocking=True, token=token):
        with self._redis.lock(lock_name,
                              timeout=self._timeout_,
                              blocking_timeout=self._blocking_timeout,
                              thread_local=False):

            temp = self._redis.hget(item_id, "common_dict")
            w_common_dict = pickle.loads(temp) if temp is not None else dict()
            temp = self._redis.hget(item_id, "msg_dict")
            w_msg_dict = pickle.loads(temp) if temp is not None else dict()

            # Удаляем из словарей данные, соответствующие этому сообщению, если конечно это сообщение не новое
            try:
                del w_msg_dict[msg_id]
            except KeyError:
                # Если сообщения не было, то и нет нужды чистить common_dict
                pass
            else:
                # Сначала проверяем элемент по-умолчанию: если common_dict[None] - существует, то
                # значит текуший элемент по-умолчанию будет заменен, необходимо проверить
                # не останется ли он пустым
                # и соответственно не нужно ли его удалить
                try:
                    new_def_el = common_dict[None]
                except KeyError:
                    pass
                else:
                    try:
                        cur_def_el = w_common_dict[None]
                    except KeyError:
                        pass
                    else:
                        # Если флаг is_single выставлен, это означает, что этот элемент
                        # единственный и без него common_dict и msg_dict - пустые, а значит,
                        # чтоб не захламлять хранилище, нужно удалить данные соответствующие прежнему сообщению
                        try:
                            is_single = cur_def_el['is_single']
                        except KeyError:
                            pass
                        except TypeError:
                            pass
                        else:
                            if is_single:
                                last_msg_id = cur_def_el['msg_id']
                                # Затем удаляем None-элемент в общем списке
                                try:
                                    del w_common_dict[None]
                                except KeyError:
                                    pass
                                # А затем удаляем и само сообщение
                                try:
                                    del w_msg_dict[last_msg_id]
                                except KeyError:
                                    pass

                temp_list = list()
                for k, v in w_common_dict.items():
                    try:
                        if v['msg_id'] == msg_id:
                            temp_list.append(k)
                    except KeyError:
                        pass
                for k in temp_list:
                    del w_common_dict[k]

            # Вставляем данные только если есть управляющие данные для обработки
            if len(msg_dict) > 0 or len(common_dict) > 0:

                # Если элемент по умолчанию (key == None) такой один,
                # то выставляем соответствующий флаг (is_single)
                if len(msg_dict) == 0 and set(common_dict.keys()) == {None}:
                    is_single = True
                else:
                    is_single = False

                for k, v in common_dict.items():
                    v['msg_id'] = msg_id
                    if k is None:
                        v['is_single'] = is_single
                    w_common_dict[k] = v

                w_msg_dict[msg_id] = dict(msg_obj=msg_obj, msg_dict=msg_dict, alias_msg=alias_msg)

                # Сохраняем в хэш алиасов
                aliases_dict = dict(dict_id=dict_id, msg_id=msg_id)
                self._redis.hset("aliases_dict", alias_msg, pickle.dumps(aliases_dict))
            else:
                # Удаляем алиас сообщения
                try:
                    self._redis.hdel("aliases_dict", alias_msg)
                except:
                    pass

            # Сохраняем словари
            self._redis.hset(item_id, "common_dict", pickle.dumps(w_common_dict))
            self._redis.hset(item_id, "msg_dict", pickle.dumps(w_msg_dict))


def unlock(lock, token=None):
    if token is None:
        try:
            lock.release()
        except:
            return False if lock.locked() else True
        else:
            return True
    else:
        try:
            lock.do_release(token)
        except:
            return False if lock.locked() else True
        else:
            return True


class MsgGetterRedis(MsgGetter):
    """ Класс объекта хранилища данных сообщений и контекста этих сообщений с использованием Redis
    Все ссылки на данные о сообщениях сделаны не на основе msg_id, а на основе alias_msg
    Планируется организовать 2 лока: главный и локальный.
    Главный - главный лок, который должен препятствовать повторной обработке
            при дублированной отправке сообщения, действует с инициализации ContextConnector
            и до выполнения деструктора
    Локальный - для блокировки ханилища на время
    обработки конкретного сообщения, особенностью этого лока будет. что токен для разблокировки
    его будет привязан к адресуконкретного сообщения или к алиасу.
    Работу с данными сообщений планируется построить на принципе журналируемого ханилища.
    То есть в данном объекте будут храниться 2 версии данных для сообщений: ТЕКУЩЕЙ и ПЛАНИРУЕМОЙ.
    Поскольку ввиду оганичений платформ различных мессенжеров сообщения могут быть отправлены с
    определенными интервалами, а данные всех сгенерированных сообщений известны
    за исключением только msg_id, который присваивается на этапе отправки сообщения
    платформой пользователю, соответственно в ТЕКУЩЕЙ версии данных будет данные,
    учитывающие все отправленные сообщения, а в ПЛАНИРУЕМО - данные на основе ТЕКУЩЕЙ версии данных,
    а также всех сообщений планируемых к отправлению объектом sender.
    """

    # Суффикс для строки названия главного
    _SUF_MAIN_LOCK = b'_main_lock'
    _SUF_LOCAL_LOCK = b'_local_lock'
    _SUF_GET_MSG_LOCK = b'_get_msg_lock'

    def __init__(self, common_dict_def, redis_host='localhost', redis_port=6379, redis_db=0,
                 timeout=300, blocking_timeout=310, redis_username=None, redis_password=None):
        """ Если какая-то команда отсутствует, она может быть взята из common_dict_def """
        super().__init__(common_dict_def)
        # self._redis_host = redis_host
        # self._redis_port = redis_port
        # self._redis_db = redis_db
        self._timeout_ = timeout
        self._blocking_timeout = blocking_timeout
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password)

    def acquire_main_lock(self, dict_id):
        """Захватываем главный лок, который должен препятствовать повторной обработке
            при дублированной отправке сообщения, действует с инициализации ContextConnector
            и до выполнения деструктора
            Лок может быть захвачен при соблюдении 2 условий:
            1) Данный лок на текущий момент свободен
            2) ТЕКУЩАЯ версия данных и ПЛАНИРУЕМАЯ одинаковы, то есть для данного dict_id нет сообщений
                планируемых к отправке в Sender e
        """
        if dict_id is None:
            return
        token = get_hash(dict_id)
        itemid = str(token).encode()
        main_lock_name = itemid + self._SUF_MAIN_LOCK
        local_lock_name = itemid + self._SUF_LOCAL_LOCK

        # Проверяем занят ли главный лок, если да, то ждем его освобождения
        main_lock = self._redis.lock(main_lock_name,
                                     timeout=self._timeout_,
                                     blocking_timeout=self._blocking_timeout,
                                     thread_local=False)

        if main_lock.acquire(blocking=True, token=token):
            # пытаемся захватить локальный лок (на случай если сообщение в стадии обработки кем-то еще)
            local_lock = self._redis.lock(local_lock_name,
                                          timeout=self._timeout_,
                                          blocking_timeout=self._blocking_timeout,
                                          thread_local=True)
            if local_lock.acquire(blocking=True):
                # Проверяем не занят ли семафор (семафор - это количество сообщений на отправку
                # объектом sender)
                semaphore = self._redis.hget('semaphore', itemid)
                is_exception = True if semaphore is not None and semaphore > b"0" else False
                # освобождаем локальный лок
                local_lock.release()
                if is_exception:
                    main_lock.do_release(token)
                    raise IsMsgProcessError("Message is processing, semaphore={}".format(semaphore))

    def release_main_lock(self, dict_id):
        """ Освобождает главный лок """
        if dict_id is None:
            return

        token = get_hash(dict_id)
        itemid = str(token).encode()
        lock_name = itemid + self._SUF_MAIN_LOCK

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)
        # Снимаем блокировку
        if lock.locked():
            lock.do_release(token)

    def get_dict_id_by_alias(self, alias_msg):
        """ Получение dict_id и msg_id по значению алиаса """
        # Работаем с msg_dict
        temp = self._redis.hget("actual_msg_dict", alias_msg)
        if temp is not None:
            try:
                temp_msg = pickle.loads(temp)
            except:
                raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg))
        else:
            raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg))

        try:
            return temp_msg['dict_id']
        except KeyError:
            raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg))



    def get_msg_by_alias(self, alias_msg, dict_id, command=None):
        # Все 3 параметра не могут быть равны None одновременно,
        # если dict_id=None и command=None, то ищем dict_id и msg_id по alias_msg,
        # если alias_msg=None то ищем данные сообщения в command_dict

        if dict_id is None or not isinstance(dict_id, dict):
            raise MsgGetterError('get_msg_by_alias arguments error')

        # if alias_msg is None:
        #     if dict_id is None or not isinstance(dict_id, dict):
        #         raise MsgGetterError('get_msg_by_alias arguments error')
        # else:
        #     # Находим dict_id и msg_id
        #     temp_dict = self._redis.hget("aliases_dict", alias_msg)
        #     if type(temp_dict) is dict:
        #         msg_id = temp_dict['msg_id']
        #         dict_id_in_alias = temp_dict['dict_id']
        #         if dict_id_in_alias != dict_id:
        #             raise MsgGetterError("Troubles is dict_id by alias_id")
        #     else:
        #         raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg))

        # В качестве токена для локального лока, необходимо использовать alias_msg, для того чтобы
        # в методе set_msg можно было разблокировань только если этот сеттер вызван для
        # конкретного сообщения. Однако на текущем этапе alias_msg может быть неизвестен,
        # а чтобы его получить необходимо получить доступ к common_dict, а для этого захватить лок.
        # Решением этого противоречия может служить подход, при котором производится блокирование лока
        # с произвольным токеном, а затем, когда alias_msg станет известным, разблокировка этого лока
        # и его повторная блокировка с token=alias_msg. Проблема в том, что при таком подходе,
        # локальный лок на короткий промежуток времени остается разблокирован и потенциально может быть
        # захвачен другим объектом или методом. Для решения этой проблемы ввдоится get_msg_lock
        # с именем get_msg_lock_name.

        item_id = str(get_hash(dict_id)).encode()
        local_lock_name = item_id + self._SUF_LOCAL_LOCK
        inside_lock_name = item_id + self._SUF_GET_MSG_LOCK

        local_lock = self._redis.lock(local_lock_name,
                                      timeout=self._timeout_,
                                      blocking_timeout=self._blocking_timeout,
                                      thread_local=False)

        inside_lock = self._redis.lock(inside_lock_name,
                                       timeout=self._timeout_,
                                       blocking_timeout=self._blocking_timeout,
                                       thread_local=False)

        # продолжаем ожидание до тех пор пока local_lock и get_msg_lock
        # не окажутся свободны одновременно
        while True:
            if local_lock.acquire(blocking=True):
                if inside_lock.locked():
                    local_lock.release()
                else:
                    break
                # Ждем освобождения вспомогательного лока
                if inside_lock.acquire(blocking=True):
                    inside_lock.release()

        # Работоем под локом
        temp = self._redis.hget("actual_common_dict", item_id)
        w_common_dict = pickle.loads(temp) if temp is not None else dict()

        if alias_msg is None:
            try:
                temp_dict = w_common_dict[command]
            except KeyError:
                try:
                    temp_dict = self._get_control(dict_id, command)
                    # temp_dict = self._common_dict_def[command]
                except KeyError:
                    unlock(local_lock)
                    raise NoMatchCommandError("Command '{}' is empty".format(command))
                else:
                    t = deepcopy(temp_dict)
                    res = dict(vertex_name=t.get('vertex_name', None),
                               control_dict=t.get('control_dict', dict())), \
                          None
                    unlock(local_lock)
                    return res
            try:
                alias_msg_new = temp_dict['alias_msg']
            except KeyError:
                unlock(local_lock)
                raise MsgGetterError("Common dict format error (there is not 'alias_msg' key)")
        else:
            alias_msg_new = alias_msg
            temp_dict = None  # будет обработано дальше, когда будут считаны данные сообщения

        # Работаем с msg_dict
        temp = self._redis.hget("actual_msg_dict", alias_msg_new)
        if temp is not None:
            temp_msg = pickle.loads(temp)
        else:
            unlock(local_lock)
            raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg_new))

        try:
            msg_obj = deepcopy(temp_msg.get('msg_obj', None))
            msg_dict = deepcopy(temp_msg['msg_dict'])
            msg_id = temp_msg['msg_id']
        except KeyError:
            unlock(local_lock)
            raise MsgGetterError("Msg_dict format error (there is not 'alias_msg' key)")
        else:
            if temp_dict is None:  # это может быть, если команда была в msg_dict, а не в common_dict
                if command is not None:
                    try:
                        temp_dict = msg_dict[command]
                    except KeyError:
                        unlock(local_lock)
                        raise NoMatchCommandError("Command '{}' is empty in msg_dict".format(command))
                # После этого блока останется только один вариант чтоб temp_dict == None
                # - если  command = None

        # Получаем common_dict
        common_dict = dict()
        for k, v in w_common_dict.items():
            try:
                if v['alias_msg'] == alias_msg_new:
                    common_dict[k] = dict(vertex_name=v['vertex_name'],
                                          control_dict=deepcopy(v['control_dict']))
            except KeyError:
                pass

        # Перезапускаем лок с токеном=alias_msg
        if inside_lock.acquire(blocking=True):
            unlock(local_lock)
            if local_lock.acquire(blocking=True, token=alias_msg_new):
                inside_lock.release()

        if temp_dict is None:
            # Этот вариант сработает только в случае, если command == None
            return None, \
                   dict(dict_id=dict_id,
                        msg_id=msg_id,
                        msg_obj=msg_obj,
                        common_dict=common_dict,
                        msg_dict=msg_dict,
                        alias_msg=alias_msg_new)
        else:
            t = deepcopy(temp_dict)
            return dict(vertex_name=t.get('vertex_name', None),
                        control_dict=t.get('control_dict', dict())), \
                   dict(dict_id=dict_id,
                        msg_id=msg_id,
                        msg_obj=msg_obj,
                        common_dict=common_dict,
                        msg_dict=msg_dict,
                        alias_msg=alias_msg_new,
                        is_msg_changed=False,
                        is_msg_dict=False)

    def get_msg(self, dict_id, msg_id, command):
        # Получаем alias_msg
        if dict_id is not None and msg_id is not None:
            alias_msg = self._redis.hget("aliases_dict",
                                         str(get_hash((dict_id, msg_id))).encode())
            if alias_msg is None:
                raise NoSuchMsgError("No such message had dict_id={}, msg_id={}".format(dict_id, msg_id))
        else:
            alias_msg = None
        return self.get_msg_by_alias(alias_msg=alias_msg, dict_id=dict_id, command=command)

    def set_msg(self, dict_msg_arg):
        """ dict_msg_arg = dict(dict_id=dict_id, msg_id=msg_id_new,
                                msg_obj=msg_obj, common_dict=common_dict, msg_dict=msg_dict) """

        try:
            dict_id = deepcopy(dict_msg_arg['dict_id'])
            msg_id = deepcopy(dict_msg_arg['msg_id'])
            common_dict = deepcopy(dict_msg_arg['common_dict'])
            msg_dict = deepcopy(dict_msg_arg['msg_dict'])
            msg_obj = deepcopy(dict_msg_arg['msg_obj'])
            alias_msg = dict_msg_arg['alias_msg']
        except KeyError:
            return

        # Инициализация локов
        item_id = str(get_hash(dict_id)).encode()
        local_lock_name = item_id + self._SUF_LOCAL_LOCK
        inside_lock_name = item_id + self._SUF_GET_MSG_LOCK

        local_lock = self._redis.lock(local_lock_name,
                                      timeout=self._timeout_,
                                      blocking_timeout=self._blocking_timeout,
                                      thread_local=False)

        inside_lock = self._redis.lock(inside_lock_name,
                                       timeout=self._timeout_,
                                       blocking_timeout=self._blocking_timeout,
                                       thread_local=False)
        token = alias_msg

        # Пытаемся освободить local_lock, это можно сделать только если он был заблокирован
        # при методом get_msg для того же сообщения (token = alias_msg).
        # Если не получается разблокировати,то ждем:

        while True:
            if inside_lock.acquire(blocking=True):
                if unlock(local_lock, token):
                    # Если получилось разлочить local_lock, то сразу
                    # занимаем его c другим токеном и выходим из цикла
                    if local_lock.acquire(blocking=True):
                        inside_lock.release()
                        break
                else:
                    # Если не получилось разлочить, то ждем его освобождения,
                    # предварительно освобождая внутренний лок
                    inside_lock.release()
                    if local_lock.acquire(blocking=True):
                        local_lock.release()
                        continue

        # Работаем под локом
        temp = self._redis.hget("actual_common_dict", item_id)
        w_common_dict = pickle.loads(temp) if temp is not None else dict()

        # temp = self._redis.hget("actual_msg_dict", alias_msg)
        # w_msg_dict = pickle.loads(temp) if temp is not None else dict()

        # Сначала проверяем элемент по-умолчанию: если common_dict[None] - существует, то
        # значит текуший элемент по-умолчанию будет заменен, необходимо проверить
        # не останется ли он пустым
        # и соответственно не нужно ли его удалить
        try:
            new_def_el = common_dict[None]
        except KeyError:
            pass
        else:
            try:
                cur_def_el = w_common_dict[None]
            except KeyError:
                pass
            else:
                # Если флаг is_single выставлен, это означает, что этот элемент
                # единственный и без него common_dict и msg_dict - пустые, а значит,
                # чтоб не захламлять хранилище, нужно удалить данные соответствующие прежнему сообщению
                try:
                    is_single = cur_def_el['is_single']
                except KeyError:
                    pass
                except TypeError:
                    pass
                else:
                    if is_single:
                        last_alias_msg = cur_def_el['alias_msg']
                        # Затем удаляем None-элемент в общем списке
                        try:
                            del w_common_dict[None]
                        except KeyError:
                            pass
                        # А затем удаляем и само сообщение
                        try:
                            self._redis.hdel("actual_msg_dict", last_alias_msg)
                        except:
                            pass

            temp_list = list()
            for k, v in w_common_dict.items():
                try:
                    if v['alias_msg'] == alias_msg:
                        temp_list.append(k)
                except KeyError:
                    pass
            for k in temp_list:
                del w_common_dict[k]

        # Вставляем данные только если есть управляющие данные для обработки
        if len(msg_dict) > 0 or len(common_dict) > 0:

            # Если элемент по умолчанию (key == None) такой один,
            # то выставляем соответствующий флаг (is_single)
            if len(msg_dict) == 0 and set(common_dict.keys()) == {None}:
                is_single = True
            else:
                is_single = False

            for k, v in common_dict.items():
                v['alias_msg'] = alias_msg
                if k is None:
                    v['is_single'] = is_single
                w_common_dict[k] = v

            self._redis.hset("actual_msg_dict", alias_msg, pickle.dumps(dict(msg_obj=msg_obj,
                                                                             msg_dict=msg_dict,
                                                                             msg_id=msg_id,
                                                                             dict_id=dict_id)))

            # хэш алиасов не сохраняем, поскольку msg_id может быть = None
            # self._redis.hset("aliases_dict", alias_msg, pickle.dumps(aliases_dict))

        else:
            # Удаляем алиас сообщения если оно задано
            if msg_id is not None:
                try:
                    self._redis.hdel("aliases_dict", str(get_hash((dict_id, msg_id))).encode())
                except:
                    pass

            # Удаляем сообщение, если возможно
            try:
                self._redis.hdel("actual_msg_dict", alias_msg)
            except:
                pass

        # Увеличиваем семафор
        self._redis.hincrby("semaphore", item_id, 1)

        # Сохраняем w_common_dict
        self._redis.hset("actual_common_dict", item_id, pickle.dumps(w_common_dict))

        # Освобождаем лок
        unlock(local_lock)

    def send_msg(self, alias_msg, dict_id, msg_id, is_sending_err=False, state=0, is_new_msg=True):
        """ Метод вызвается при физической отправке сообщения,
            когда становится известным его msg_id,
            если msg_id==None, то значит сообщение было удалено
            is_sending_err - флаг, обозначающий ошибки при отправке,
            в этом случае msg_id тоже == None"""
        if alias_msg is None:
            return
        if dict_id is None:
            return

        # Инициализация локов
        item_id = str(get_hash(dict_id)).encode()
        local_lock_name = item_id + self._SUF_LOCAL_LOCK
        inside_lock_name = item_id + self._SUF_GET_MSG_LOCK

        local_lock = self._redis.lock(local_lock_name,
                                      timeout=self._timeout_,
                                      blocking_timeout=self._blocking_timeout,
                                      thread_local=False)

        inside_lock = self._redis.lock(inside_lock_name,
                                       timeout=self._timeout_,
                                       blocking_timeout=self._blocking_timeout,
                                       thread_local=False)

        # продолжаем ожидание до тех пор пока local_lock и inside_lock
        # не окажутся свободны одновременно
        while True:
            if local_lock.acquire(blocking=True):
                if inside_lock.locked():
                    local_lock.release()
                else:
                    break
                # Ждем освобождения вспомогательного лока
                if inside_lock.acquire(blocking=True):
                    inside_lock.release()

        # Работаем под защитой лока local_lock

        # Находим и уменьшаем семафор
        try:
            self._redis.hincrby("semaphore", item_id, -1)
        except Exception as e:
            # unlock(local_lock)
            # raise MsgGetterError("Semaphore error in msg with alias_msg={}".format(alias_msg))
            logging.warning('Problem with semaphore dict_id={}, error: {}'.format(dict_id, e))
            pass

        # Если стало известно msg_id, записываем эту информацию
        # в aliases_dict и actual_msg_dict
        if msg_id is not None:
            temp = self._redis.hget("actual_msg_dict", alias_msg)
            if temp is not None:
                self._redis.hset("aliases_dict",
                                 str(get_hash((dict_id, msg_id))).encode(),
                                 alias_msg)
                temp_msg = pickle.loads(temp)

                temp_msg['msg_id'] = msg_id
                temp_msg['dict_id'] = dict_id   # хотя dict_id и так уже исзвестен
                self._redis.hset("actual_msg_dict", alias_msg, pickle.dumps(temp_msg))
        else:
            # Удаляем сообщение
            self._redis.hdel("actual_msg_dict", alias_msg)

            # Очищаем common_dict
            temp = self._redis.hget("actual_common_dict", item_id)
            w_common_dict = pickle.loads(temp) if temp is not None else dict()
            temp_list = list()
            for k, v in w_common_dict.items():
                try:
                    if v['alias_msg'] == alias_msg:
                        temp_list.append(k)
                except KeyError:
                    pass
            for k in temp_list:
                del w_common_dict[k]
            # Сохраняем w_common_dict
            self._redis.hset("actual_common_dict", item_id, pickle.dumps(w_common_dict))

        # Освобождаем лок
        unlock(local_lock)


class SenderMsgRedisOld(SenderMsg):
    """ Класс объекта отвечающего за отправку сообщений.
        Обработка очереди сообщений, выполняется в отдельном потоке одного (только Одного!!!)
        из процессов-работников программы """

    MAIN_QUEUE_LOCK_NAME = 'main_queue_lock_modern'
    LOCAL_QUEUE_LOCK_NAME = 'local_queue_lock_modern'
    KEY_QUEUE = 'queue'
    KEY_ERROR_QUEUE = 'err_queue'
    _thread = None

    def __init__(self, sender=None, msg_getter=None, redis_host='localhost', redis_port=6379, redis_db=1,
                 time_to_sleep=0.5, timeout_main_lock=None,
                 time_begin=0., time_end=5., redis_username=None, redis_password=None):
        """
        :param sender - объект-функция реальной отправки сообщения
        :param msg_getter - объект потомок MsgGetter
        :param redis_host - параметр для подключения к redis
        :param redis_port - параметр для подключения к redis
        :param redis_db - параметр для подключения к redis (номер базы данных, где хранится очередь)
        :param time_to_sleep - длительность паузы между отправками сообщений
        :param timeout_main_lock - время, на которое блокируется БД redis, в которой хранится очередь
        :param time_begin - начало интервала, из которого берется значения случайного интервала запуска обработчика
        :param time_end  - конец интервала, из которого берется значения случайного интервала запуска обработчика
        Случайное значение необходимо, чтобы исключить конфликт за захват главного лока, поскольку обработку очереди
        должен обеспечить только один SenderMsg
        """
        assert isinstance(msg_getter, MsgGetterOld) or msg_getter is None
        assert isinstance(sender, AbstractSender) or sender is None
        self._msg_getter = msg_getter

        self._sender = sender
        self._time_to_sleep = time_to_sleep
        if timeout_main_lock is not None and timeout_main_lock > time_to_sleep:
            self._timeout_main_lock = timeout_main_lock
        else:
            self._timeout_main_lock = time_to_sleep + 1.
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password)

        self._token = str(uuid.uuid1())
        self._main_lock = self._redis.lock(self.__class__.MAIN_QUEUE_LOCK_NAME,
                                           timeout=self._timeout_main_lock,
                                           blocking_timeout=self._timeout_main_lock,
                                           thread_local=False)

        self._local_token = str(uuid.uuid1())
        self._local_lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME,
                                            timeout=self._timeout_main_lock,
                                            blocking_timeout=self._timeout_main_lock,
                                            thread_local=False)

        self._thread = None
        # self._local_lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME, thread_local=False)
        # Пытаемся сразу запустить handle_function.
        # Для избежания конфликтов, делаем это в случаное время
        sleep(uniform(time_begin, time_end))
        if not self._main_lock.locked():
            self.handle_function()

    def handle_function(self):
        # with self._local_lock.acquire(blocking=True):
        # lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME,
        #                         timeout=10,
        #                         blocking_timeout=10,
        #                         thread_local=False)
        #                                                                          lock.locked()))
        # with self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME, timeout=10, thread_local=False):

        # Захватываем локальный лок
        try:
            self._local_lock.acquire(blocking=True, token=self._local_token)
        except Exception:
            return

        if self._main_lock.locked():
            # Пытаемся разблокировать главный лок, это может быть сделано, только если
            # внутренний token лока и self._token совпадают
            try:
                self._main_lock.do_release(self._token)
            except Exception:
                # освобождаем локальный лок и выходим
                try:
                    self._local_lock.do_release(self._local_token)
                except Exception:
                    pass
                return
        # if not self._main_lock.locked():

        # Как только освободился сразу захватываем главный лок со своим токеном
        self._main_lock.acquire(blocking=True, token=self._token)

        # обработка очереди
        self._handle_queue()

        # освобождаем локальный лок
        try:
            self._local_lock.do_release(self._local_token)
        except Exception:
            pass

        self._thread = Timer(self._time_to_sleep, self.handle_function)
        self._thread.start()

    def stop_handle(self):
        if self._thread is not None:
            self._thread.cancel()
            if self._main_lock.locked():
                try:
                    self._main_lock.do_release(self._token)
                except Exception:
                    return

            # освобождаем локальный лок
            if self._local_lock.locked():
                try:
                    self._local_lock.do_release(self._local_token)
                except Exception:
                    pass

    def _handle_queue(self):
        """ Обработчик очереди """
        if self._msg_getter is None or self._sender is None:
            return
        serialized_v = self._redis.rpop(self.__class__.KEY_QUEUE)
        if serialized_v is not None:
            v = pickle.loads(serialized_v)
            dict_id = v.get('dict_id', None)
            try:
                msg_id, state = self._sender(dict_id, v['msg_id'], v['msg_obj'])
            except SenderError as e:
                # Если какие-то ошибки при физической отправке сообщения,
                # вставляем данные этого сообщения в очередь ошибок
                logging.warning(' -! SenderError e = {}'.format(e))
                self._redis.lpush(self.__class__.KEY_ERROR_QUEUE, serialized_v)
            except KeyError:
                pass
            else:
                v['msg_id'] = msg_id
                if state == 3:
                    v['common_dict'] = dict()
                    v['msg_dict'] = dict()
                try:
                    self._msg_getter.set_msg(v)
                except Exception as e:
                    pass
            finally:
                if dict_id is not None:
                    self._msg_getter.dec_semaphore(v['dict_id'])

    def __call__(self, msg_context_obj):
        if self._msg_getter is None:
            return
        # with self._local_lock.acquire(blocking=True):
        # with self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME, thread_local=False):
        try:
            self._local_lock.acquire(blocking=True, token=self._local_token)
        except Exception:
            return

        # if isinstance(self._msg_getter, MsgGetterRedis):
        try:
            dict_id = msg_context_obj['dict_id']
        except Exception:
            pass
        else:
            # is_msg_changed=True, is_msg_dict=True
            if msg_context_obj.get('is_msg_changed', True) or msg_context_obj.get('msg_id', None) is None:
                self._msg_getter.inc_semaphore(dict_id)
                self._redis.lpush(self.__class__.KEY_QUEUE, pickle.dumps(msg_context_obj))
            else:
                if msg_context_obj.get('is_msg_dict', True):
                    try:
                        self._msg_getter.set_msg(msg_context_obj)
                    except Exception as e:
                        pass

        # освобождаем локальный лок
        try:
            self._local_lock.do_release(self._local_token)
        except Exception:
            pass

        if not self._main_lock.locked():
            self.handle_function()

    def __del__(self):
        self.stop_handle()


class SenderMsgRedis(SenderMsg):
    """ Класс объекта отвечающего за отправку сообщений.
        Обработка очереди сообщений, выполняется в отдельном потоке одного (только Одного!!!)
        из процессов-работников программы """

    MAIN_QUEUE_LOCK_NAME = 'main_queue_lock_modern'
    LOCAL_QUEUE_LOCK_NAME = 'local_queue_lock_modern'
    KEY_QUEUE = 'queue'
    KEY_ERROR_QUEUE = 'err_queue'
    _thread = None

    def __init__(self, sender=None, msg_getter=None, redis_host='localhost', redis_port=6379, redis_db=1,
                 time_to_sleep=0.5, timeout_main_lock=None,
                 time_begin=0., time_end=5., redis_username=None, redis_password=None):
        """
        :param sender - объект-функция реальной отправки сообщения
        :param msg_getter - объект потомок MsgGetter
        :param redis_host - параметр для подключения к redis
        :param redis_port - параметр для подключения к redis
        :param redis_db - параметр для подключения к redis (номер базы данных, где хранится очередь)
        :param time_to_sleep - длительность паузы между отправками сообщений
        :param timeout_main_lock - время, на которое блокируется БД redis, в которой хранится очередь
        :param time_begin - начало интервала, из которого берется значения случайного интервала запуска обработчика
        :param time_end  - конец интервала, из которого берется значения случайного интервала запуска обработчика
        Случайное значение необходимо, чтобы исключить конфликт за захват главного лока, поскольку обработку очереди
        должен обеспечить только один SenderMsg
        """
        assert isinstance(msg_getter, MsgGetter) or msg_getter is None
        assert isinstance(sender, AbstractSender) or sender is None
        self._msg_getter = msg_getter

        self._sender = sender
        self._time_to_sleep = time_to_sleep
        if timeout_main_lock is not None and timeout_main_lock > time_to_sleep:
            self._timeout_main_lock = timeout_main_lock
        else:
            self._timeout_main_lock = time_to_sleep + 1.
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password)

        self._token = str(uuid.uuid1())
        self._main_lock = self._redis.lock(self.__class__.MAIN_QUEUE_LOCK_NAME,
                                           timeout=self._timeout_main_lock,
                                           blocking_timeout=self._timeout_main_lock,
                                           thread_local=False)

        self._local_token = str(uuid.uuid1())
        self._local_lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME,
                                            timeout=self._timeout_main_lock,
                                            blocking_timeout=self._timeout_main_lock,
                                            thread_local=False)

        self._thread = None
        # self._local_lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME, thread_local=False)
        # Пытаемся сразу запустить handle_function.
        # Для избежания конфликтов, делаем это в случаное время
        sleep(uniform(time_begin, time_end))
        if not self._main_lock.locked():
            self.handle_function()

    def handle_function(self):
        # with self._local_lock.acquire(blocking=True):
        # lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME,
        #                         timeout=10,
        #                         blocking_timeout=10,
        #                         thread_local=False)
        #                                                                          lock.locked()))
        # with self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME, timeout=10, thread_local=False):

        # Захватываем локальный лок
        try:
            self._local_lock.acquire(blocking=True, token=self._local_token)
        except Exception:
            return

        if self._main_lock.locked():
            # Пытаемся разблокировать главный лок, это может быть сделано, только если
            # внутренний token лока и self._token совпадают
            try:
                self._main_lock.do_release(self._token)
            except Exception:
                # освобождаем локальный лок и выходим
                try:
                    self._local_lock.do_release(self._local_token)
                except Exception:
                    pass
                return
        # if not self._main_lock.locked():

        # Как только освободился сразу захватываем главный лок со своим токеном
        self._main_lock.acquire(blocking=True, token=self._token)

        # обработка очереди
        self._handle_queue()

        # освобождаем локальный лок
        try:
            self._local_lock.do_release(self._local_token)
        except Exception:
            pass

        self._thread = Timer(self._time_to_sleep, self.handle_function)
        self._thread.start()

    def stop_handle(self):
        if self._thread is not None:
            self._thread.cancel()
            if self._main_lock.locked():
                try:
                    self._main_lock.do_release(self._token)
                except Exception:
                    return

            # освобождаем локальный лок
            if self._local_lock.locked():
                try:
                    self._local_lock.do_release(self._local_token)
                except Exception:
                    pass

    def _handle_queue(self):
        """ Обработчик очереди """
        if self._msg_getter is None or self._sender is None:
            return
        serialized_v = self._redis.rpop(self.__class__.KEY_QUEUE)
        if serialized_v is not None:
            v = pickle.loads(serialized_v)
            dict_id = v.get('dict_id', None)
            alias_msg = v.get('alias_msg', None)
            msg_id = None
            try:
                msg_id, state = self._sender(dict_id, v['msg_id'], v['msg_obj'])
            except SenderError as e:
                # Если какие-то ошибки при физической отправке сообщения,
                # вставляем данные этого сообщения в очередь ошибок
                logging.warning(' - SenderError e = {}'.format(e))
                self._redis.lpush(self.__class__.KEY_ERROR_QUEUE, serialized_v)
                # TODO: Сделать отработку очереди ошибок
            except KeyError:
                pass
            finally:
                # Метода send_msg в любом случае должен вызываться, т.к. от увеличивает семафор
                self._msg_getter.send_msg(alias_msg=alias_msg,
                                          dict_id=dict_id,
                                          msg_id=msg_id if msg_id is not None else v.get('msg_id', None))

    def __call__(self, msg_context_obj):
        if self._msg_getter is None:
            return
        # with self._local_lock.acquire(blocking=True):
        # with self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME, thread_local=False):
        try:
            self._local_lock.acquire(blocking=True, token=self._local_token)
        except Exception:
            return

        # if isinstance(self._msg_getter, MsgGetterRedis):
        try:
            dict_id = msg_context_obj['dict_id']
        except Exception:
            pass
        else:
            # is_msg_changed=True, is_msg_dict=True
            msg_id = msg_context_obj.get('msg_id', None)
            dict_id = msg_context_obj.get('dict_id', None)
            alias_msg = msg_context_obj.get('alias_msg', None)
            if msg_context_obj.get('is_msg_changed', True) or msg_id is None:
                # Выявляем удаление сообщения (msg_obj == None или == dict())
                msg_obj = msg_context_obj.get('msg_obj', None)
                if msg_obj is None or len(msg_obj) == 0:
                    msg_context_obj['common_dict'] = dict()
                    msg_context_obj['msg_dict'] = dict()
                self._msg_getter.set_msg(msg_context_obj)
                self._redis.lpush(self.__class__.KEY_QUEUE, pickle.dumps(msg_context_obj))
            else:
                # Вызываем set_msg и send_msg в любом случае, чтоб уменьшить семафор и снять лок
                self._msg_getter.set_msg(msg_context_obj)
                self._msg_getter.send_msg( alias_msg=alias_msg, dict_id=dict_id, msg_id=msg_id)

        # освобождаем локальный лок
        try:
            self._local_lock.do_release(self._local_token)
        except Exception:
            pass

        if not self._main_lock.locked():
            self.handle_function()

    def __del__(self):
        self.stop_handle()


class ContextStorageRedis(ContextStorage):
    # Суффикс для строки названия блокировки хранилища контента
    SUF_STR_CONTENT_STORAGE = b"_csv2"

    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0,
                 timeout=300, blocking_timeout=310, redis_username=None, redis_password=None, ):
        """ Если какая-то команда отсутствует, она может быть взята из common_dict_def """
        # self._redis_host = redis_host
        # self._redis_port = redis_port
        # self._redis_db = redis_db
        self._timeout_ = timeout
        self._blocking_timeout = blocking_timeout
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password)

    def get_context(self, dict_id):
        token = get_hash(dict_id)
        itemid = str(token).encode()
        lock_name = itemid + self.__class__.SUF_STR_CONTENT_STORAGE

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)

        if lock.acquire(blocking=False, token=token):
            temp = self._redis.hget(itemid, "context")
            context = pickle.loads(temp) if temp is not None else dict()

        else:
            raise IsContextProcessError("This context is busy {}".format(dict_id))
        return context

    def set_context(self, dict_id, context=None):
        token = get_hash(dict_id)
        itemid = str(token).encode()
        lock_name = itemid + self.__class__.SUF_STR_CONTENT_STORAGE

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)

        if not lock.locked():
            raise IsContextProcessError("Already unlocked {}".format(dict_id))

        if context is not None:
            self._redis.hset(itemid, "context", pickle.dumps(context))

        # Снимаем блокировку
        lock.do_release(token)


# if __name__ == "__main__":
logging.basicConfig(format=u'  -2- %(levelname)-8s [%(asctime)s] %(message)s', level=logging.WARNING)
