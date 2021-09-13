#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

from dialog_machine.interfaces import *
from dialog_machine.project_exceptions import *
from copy import deepcopy
import logging

from time import sleep
from random import uniform

import redis
import pickle
from pprint import pformat
import hashlib

from threading import Timer
import uuid
import psycopg2
import time

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


def get_arg_dict(func, args, kwargs):
    """ Возвращается параметры функции func в виде единого словаря """
    args_names = list(func.__code__.co_varnames[:func.__code__.co_argcount])
    try:
        if args_names[0] == 'self':
            args_names.pop(0)
    except:
        pass
    return {**dict(zip(args_names, args)), **kwargs}


def get_hash(value):
    """ Находит hash любого объекта python, необходимо для короткого ключа redis """
    # return str(value['chat_id']) + str(value['user_id'])
    return hashlib.md5(pformat(value).encode()).hexdigest()


def get_str(value):
    return pformat(value).encode()


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
    его будет привязан к адресу конкретного сообщения или к алиасу.
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
                 timeout=300, blocking_timeout=310, redis_username=None, redis_password=None,
                 redis_obj=None, semaphore_wait=310):
        """
        :param common_dict_def:
        :param redis_host:
        :param redis_port:
        :param redis_db:
        :param timeout:
        :param blocking_timeout:
        :param redis_username:
        :param redis_password:
        :param redis_obj:
        :param semaphore_wait: время ожидания семафора в секундах
        """

        """ Если какая-то команда отсутствует, она может быть взята из common_dict_def 
        """

        super().__init__(common_dict_def)
        # self._redis_host = redis_host
        # self._redis_port = redis_port
        # self._redis_db = redis_db
        self._timeout_ = timeout
        self._semaphore_wait = semaphore_wait
        self._blocking_timeout = blocking_timeout
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password,
                                  ssl_cert_reqs=None, ssl=True) \
            if redis_obj is None else redis_obj

    def semaphore_is_locked(self, item_id):
        semaphore = self._redis.hget(item_id, "semaphore")
        if semaphore is not None and semaphore > b"0":
            try:
                semaphore_time = int(self._redis.hget(item_id, "semaphore_time"))
            except Exception:
                self.del_semaphore(item_id)
                return False
            else:
                if (semaphore_time + self._semaphore_wait) < time.time():
                    self.del_semaphore(item_id)
                    return False
                else:
                    return True
        else:
            return False

    def inc_semaphore(self, item_id):
        self._redis.hincrby(item_id, "semaphore", 1)
        self._redis.hset(item_id, 'semaphore_time', str(int(time.time())))

    def dec_semaphore(self, item_id):
        self._redis.hincrby(item_id, "semaphore", -1)
        self._redis.hset(item_id, 'semaphore_time', str(int(time.time())))

    def del_semaphore(self, item_id):
        self._redis.hdel(item_id, "semaphore_time")
        self._redis.hdel(item_id, 'semaphore')

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
        item_id = str(token).encode()
        main_lock_name = item_id + self._SUF_MAIN_LOCK
        local_lock_name = item_id + self._SUF_LOCAL_LOCK

        # Проверяем занят ли главный лок, если да, то ждем его освобождения
        main_lock = self._redis.lock(main_lock_name,
                                     timeout=self._timeout_ * 1.5,
                                     blocking_timeout=self._blocking_timeout * 1.5,
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
                try:
                    is_exception = self.semaphore_is_locked(item_id)
                except:
                    raise IsMsgProcessError("Error in checking semaphore")
                finally:
                    # освобождаем локальный лок
                    local_lock.release()
                if is_exception:
                    main_lock.do_release(token)
                    raise IsMsgProcessError("Message is processing, semaphore!=0")
                else:
                    # Получаем alias_msg для сообщения по-умолчанию
                    temp = self._redis.hget(item_id, 'common_dict')
                    if temp is not None:
                        w_common_dict = pickle.loads(temp)
                    else:
                        # raise NoSuchMsgError("No such dict_id={}".format(dict_id))
                        return None
                    try:
                        temp_dict = w_common_dict[None]
                    except KeyError:
                        return None
                    try:
                        alias_msg = temp_dict['alias_msg']
                    except:
                        alias_msg = None
                    return alias_msg

    def release_main_lock(self, dict_id):
        """ Освобождает главный лок """
        if dict_id is None:
            return

        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self._SUF_MAIN_LOCK

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)
        # Снимаем блокировку
        if lock.locked():
            lock.do_release(token)

    def exists_dict_id(self, dict_id):
        """
        Метод проверяет есть ли в хранилище redis
        :param dict_id: идентификатор сеанса
        :return: если элемент в dict_id есть. то возвращает True, иначе False
        """
        if dict_id is None or not isinstance(dict_id, dict):
            return False
        item_id = str(get_hash(dict_id)).encode()
        return self._redis.exists(item_id) == 1

    def exists_alias_msg(self, alias_msg):
        """
        Метод проверяет есть ли в хранилище redis
        :param alias_msg: идентификатор сообщения
        :return: если элемент с alias_msg есть. то возвращает True, иначе False
        """
        if alias_msg is None:
            return False
        return self._redis.exists(alias_msg) == 1

    def get_dict_id_by_alias(self, alias_msg):
        """ Получение dict_id и msg_id по значению алиаса """
        temp = self._redis.hget(alias_msg, 'dict_id')
        if temp is not None:
            try:
                return pickle.loads(temp)
            except:
                raise NoSuchMsgError(f"No such message 2 had alias_msg={alias_msg}")
        else:
            raise NoSuchMsgError(f"No such message 1 had alias_msg={alias_msg}")

    def _get_msg_common(self, dict_id, command=None, alias_msg=None, msg_id=None):
        # Все 3 параметра не могут быть равны None одновременно,
        # если dict_id=None и command=None, то ищем dict_id и msg_id по alias_msg,
        # если alias_msg=None то ищем данные сообщения в command_dict

        if dict_id is None or not isinstance(dict_id, dict):
            raise MsgGetterError('get_msg_by_alias arguments error')

        # Необходимо найти dict_id при alias_msg!=None (dict_id сообщения может отличаться от
        # main_dict_id, который и передан в качестве аргумента 'dict_id')
        if alias_msg is not None:
            dict_id_b = self._redis.hget(alias_msg, 'dict_id')
            if dict_id_b is None:
                raise NoSuchMsgError("There isn't msg with alias={}".format(alias_msg))
            try:
                dict_id = pickle.loads(dict_id_b)
            except:
                raise MsgGetterError("Msg_dict format error (there is not 'alias_msg' key) 1")
        text_flag = False

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

        # Работаем под локом
        temp = self._redis.hget(item_id, 'common_dict')
        if temp is not None:
            w_common_dict = pickle.loads(temp)
        else:
            # raise NoSuchMsgError("No such dict_id={}".format(dict_id))
            w_common_dict = dict()

        temp_dict = None
        if alias_msg is None and msg_id is None:
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
        elif alias_msg is not None:
            text_flag = True

            alias_msg_new = alias_msg
            temp_dict = None  # будет обработано дальше, когда будут считаны данные сообщения
        else:
            # остается только вариант alias_msg is None and msg_id is not None
            temp = self._redis.hget(item_id, 'aliases_dict')
            if temp is not None:
                aliases_dict = pickle.loads(temp)
                try:
                    alias_msg_new = aliases_dict[msg_id]
                except KeyError:
                    unlock(local_lock)
                    raise NoSuchMsgError("No such message had msg_id={}".format(msg_id))
            else:
                unlock(local_lock)
                raise MsgGetterError("Common dict format error (there is not 'alias_msg' key)")

        # Работаем с msg_dict
        if alias_msg is None:
            dict_id_b = self._redis.hget(alias_msg_new, 'dict_id')
            try:
                dict_id_alias = pickle.loads(dict_id_b)
            except:
                unlock(local_lock)
                raise MsgGetterError("Msg_dict format error (there is not 'alias_msg' key) 2")
            else:
                # # Сравниваем dict_id из сообщения и из параметров, если не сходятся - выдаем ошибку
                # if dict_id_alias != dict_id:
                #     unlock(local_lock)
                #     raise MsgGetterError("Not match dict_id ({}) to dict_id from msg ({})".format(dict_id, dict_id_alias))
                dict_id = dict_id_alias
        msg_obj_b = self._redis.hget(alias_msg_new, 'msg_obj')
        msg_dict_b = self._redis.hget(alias_msg_new, 'msg_dict')
        msg_id_b = self._redis.hget(alias_msg_new, 'msg_id')
        msg_id_list_b = self._redis.hget(alias_msg_new, 'msg_id_list')
        if msg_obj_b is None or msg_dict_b is None:
            unlock(local_lock)
            raise NoSuchMsgError("No such message had alias_msg={}".format(alias_msg_new))
        else:
            try:
                msg_id = list(pickle.loads(msg_id_list_b))
            except:
                try:
                    msg_id = int(msg_id_b)
                except TypeError:
                    msg_id = None

            try:
                msg_obj = pickle.loads(msg_obj_b)
                msg_dict = pickle.loads(msg_dict_b)
            except:
                unlock(local_lock)
                raise MsgGetterError("Msg_dict format error (there is not 'alias_msg' key) 3")

            if temp_dict is None:  # это может быть, если команда была в msg_dict, а не в common_dict
                if command is not None:
                    try:
                        temp_dict = msg_dict[command]
                    except KeyError:
                        try:
                            temp_dict = self.get_control_msg_(dict_id, command)
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
                        alias_msg=alias_msg_new,
                        is_msg_changed=False,
                        is_msg_dict=False,
                        is_new_msg=False  # Флаг говорит о том, что сообщение уже не новое, а изменяемое
                        )
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
                        is_msg_dict=False,
                        is_new_msg=False  # Флаг говорит о том, что сообщение уже не новое, а изменяемое
                        )

    def get_msg_by_alias(self, alias_msg, dict_id, command=None):
        return self._get_msg_common(dict_id=dict_id, command=command, alias_msg=alias_msg)

    def get_msg(self, dict_id, msg_id, command):
        return self._get_msg_common(dict_id=dict_id, command=command, msg_id=msg_id)

    def _actualize_aliases_dict(self, item_id, msg_id, alias_msg):
        """ Приводит в актуальное состояние aliases_dict"""
        # if msg_id is None:
        #     return
        temp = self._redis.hget(item_id, 'aliases_dict')
        if temp is not None:
            try:
                aliases_dict = pickle.loads(temp)
            except:
                aliases_dict = dict()
        else:
            aliases_dict = dict()
        # находим все msg_id, соответствующие данному alias_msg
        msg_id_for_cur_alias = [kk for kk, vv in aliases_dict.items() if vv == alias_msg]
        for vv in set(msg_id_for_cur_alias):
            try:
                del aliases_dict[vv]
            except:
                pass
        if msg_id is not None:
            if isinstance(msg_id, list):
                for vv in msg_id:
                    try:
                        aliases_dict[int(vv)] = alias_msg
                    except:
                        continue
            else:
                try:
                    aliases_dict[int(msg_id)] = alias_msg
                except:
                    pass
        self._redis.hset(item_id, 'aliases_dict', pickle.dumps(aliases_dict))

    def set_msg(self, dict_msg_arg: dict):
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

        is_new_msg_ = dict_msg_arg.get('is_new_msg', False)

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
        temp = self._redis.hget(item_id, 'common_dict')
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
                            # self._redis.hdel("actual_msg_dict", last_alias_msg)
                            try:
                                self._redis.delete(last_alias_msg)
                            except:
                                pass
                            self._actualize_aliases_dict(item_id, None, last_alias_msg)
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

            self._redis.hset(alias_msg, 'msg_obj', pickle.dumps(msg_obj))
            self._redis.hset(alias_msg, 'msg_dict', pickle.dumps(msg_dict))
            if msg_id is not None:
                if isinstance(msg_id, list):
                    self._redis.hdel(alias_msg, 'msg_id')
                    self._redis.hset(alias_msg, 'msg_id_list', pickle.dumps(msg_id))
                elif msg_id is not None:
                    self._redis.hdel(alias_msg, 'msg_id_list')
                    self._redis.hset(alias_msg, 'msg_id', msg_id)
                else:
                    self._redis.hdel(alias_msg, 'msg_id_list')
                    self._redis.hdel(alias_msg, 'msg_id')
            else:
                self._redis.hdel(alias_msg, 'msg_id')
                self._redis.hdel(alias_msg, 'msg_id_list')
            self._redis.hset(alias_msg, 'dict_id', pickle.dumps(dict_id))

            # приводим в актуальное состояние aliases_dict
            if msg_id is not None:
                self._actualize_aliases_dict(item_id, msg_id, alias_msg)

            # хэш алиасов не сохраняем, поскольку msg_id может быть = None
            # self._redis.hset("aliases_dict", alias_msg, pickle.dumps(aliases_dict))

        else:
            # Удаляем алиас сообщения если оно задано
            if msg_id is not None:
                self._actualize_aliases_dict(item_id, None, alias_msg)
                # temp = self._redis.hget(item_id, 'aliases_dict')
                # if temp is not None:
                #     try:
                #         aliases_dict = pickle.loads(temp)
                #     except:
                #         pass
                #     else:
                #         if isinstance(msg_id, list):
                #             for msg_id_item in msg_id:
                #                 try:
                #                     del aliases_dict[msg_id_item]
                #                 except KeyError:
                #                     continue
                #             self._redis.hset(item_id, 'aliases_dict', pickle.dumps(aliases_dict))
                #         else:
                #             try:
                #                 del aliases_dict[msg_id]
                #             except KeyError:
                #                 pass
                #             else:
                #                 self._redis.hset(item_id, 'aliases_dict', pickle.dumps(aliases_dict))

            # Удаляем сообщение, если возможно
            try:
                self._redis.delete(alias_msg)
            except:
                pass

        # Увеличиваем семафор только если это новое сообщение, а не измененное
        if is_new_msg_:
            self.inc_semaphore(item_id)

        # Сохраняем w_common_dict
        self._redis.hset(item_id, 'common_dict', pickle.dumps(w_common_dict))

        # Освобождаем лок
        unlock(local_lock)

    def send_msg(self, alias_msg, dict_id, msg_id, is_sending_err=False, state=0, is_new_msg=True):
        """ Метод вызвается при физической отправке сообщения,
            когда становится известным его msg_id,
            если msg_id==None, то значит сообщение было удалено
            is_sending_err - флаг, обозначающий ошибки при отправке,
            в этом случае msg_id тоже == None
            :param state: 3 - сообщение удаляется
            :param is_sending_err: флаг, обозначающий ошибки при отправке
            :param msg_id: полученное id сообщения
            :param dict_id:
            :param alias_msg: алиас сообщения
            :param is_new_msg: True если сообщение новое, False - если сообщение измененное"""

        if is_new_msg:
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
                self.dec_semaphore(item_id)
            except Exception as e:
                # unlock(local_lock)
                # raise MsgGetterError("Semaphore error in msg with alias_msg={}".format(alias_msg))
                logging.warning('Problem with semaphore dict_id={}, error: {}'.format(dict_id, e))
                pass

            # Если стало известно msg_id, записываем эту информацию
            # в aliases_dict и actual_msg_dict
            if msg_id is not None and state != 3:  # state==3 когда сообщение удаляется
                temp = self._redis.hget(alias_msg, 'dict_id')  # поскольку dict_id может быть = None сам по себе
                if temp is not None:
                    self._actualize_aliases_dict(item_id, msg_id, alias_msg)
                    if isinstance(msg_id, list):
                        self._redis.hdel(alias_msg, 'msg_id')
                        self._redis.hset(alias_msg, 'msg_id_list', pickle.dumps(msg_id))
                    elif msg_id is not None:
                        self._redis.hset(alias_msg, 'msg_id', msg_id)
                        self._redis.hdel(alias_msg, 'msg_id_list')
                    else:
                        self._redis.hdel(alias_msg, 'msg_id_list')
                        self._redis.hdel(alias_msg, 'msg_id')
            else:
                res_msg_id_list = list()
                if msg_id is not None:
                    try:
                        msg_id_set = set(pickle.loads(self._redis.hget(alias_msg, 'msg_id_list')))
                    except:
                        pass
                    else:
                        msg_id_t = set(msg_id) if isinstance(msg_id, list) else {msg_id}
                        res_msg_id_list = list(msg_id_set - msg_id_t)
                self._actualize_aliases_dict(item_id=item_id,
                                             msg_id=res_msg_id_list,
                                             alias_msg=alias_msg)
                if len(res_msg_id_list) == 0:
                    self._redis.delete(alias_msg)
                else:
                    self._redis.hdel(alias_msg, 'msg_id')
                    self._redis.hset(alias_msg, 'msg_id_list', pickle.dumps(res_msg_id_list))

                # Очищаем common_dict
                temp = self._redis.hget(item_id, "common_dict")
                try:
                    w_common_dict = pickle.loads(temp)
                except:
                    w_common_dict = dict()
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
                self._redis.hset(item_id, "common_dict", pickle.dumps(w_common_dict))

            # Освобождаем лок
            unlock(local_lock)

    def get_data(self, dict_id):
        """
        Получаем весь сеанс
        :param dict_id: - идентификатор сеанса с пользователем
        :return: - dict(common_dict=common_dict, msg_list=msg_list)
        """
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

        if self.semaphore_is_locked(item_id):
            unlock(local_lock)
            raise IsMsgProcessError("(get_data) Message is processing, "
                                    "dict_id={}, semaphore!=0".format(dict_id))
        common_dict_b = self._redis.hget(item_id, 'common_dict')
        temp_d = self._redis.hget(item_id, 'aliases_dict')
        msg_list = list()
        if temp_d is not None:
            aliases_dict = pickle.loads(temp_d)

            for alias_msg in set(aliases_dict.values()):
                msg_obj_b = self._redis.hget(alias_msg, 'msg_obj')
                msg_dict_b = self._redis.hget(alias_msg, 'msg_dict')
                try:
                    msg_id = list(set(pickle.loads(self._redis.hget(alias_msg, 'msg_id_list'))))
                except:
                    try:
                        msg_id = int(self._redis.hget(alias_msg, 'msg_id'))
                    except:
                        msg_id = None

                msg_list.append(dict(dict_id=dict_id, msg_id=msg_id,
                                     msg_obj=msg_obj_b, msg_dict=msg_dict_b, alias_msg=alias_msg))
                self._redis.delete(alias_msg)
        if common_dict_b is None:
            common_dict_b = pickle.dumps(dict())
        self._redis.hdel(item_id, 'common_dict')
        self._redis.hdel(item_id, 'semaphore')
        self.del_semaphore(item_id)
        self._redis.hdel(item_id, 'aliases_dict')
        # if int(self._redis.hlen(item_id)) == 0:
        self._redis.delete(item_id)

        # Освобождаем лок
        unlock(local_lock)
        return dict(common_dict=common_dict_b, msg_list=msg_list)

    def put_data(self, dict_id: dict, value: dict):
        """

        :param dict_id: идентификатор сеанса с пользователем
        :param value: значение из архива
        :return:
        """
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

        common_dict_b = value['common_dict']
        msg_list = value['msg_list']

        if common_dict_b is None:
            common_dict_b = pickle.dumps(dict())
        if msg_list is None:
            msg_list = list()

        # Вставляем сообщения
        aliases_dict = dict()
        for v in msg_list:
            try:
                msg_obj_b = v['msg_obj']
                msg_dict_b = v['msg_dict']
                msg_id = v['msg_id']
                alias_msg = v['alias_msg']
            except KeyError:
                logging.warning('Problem to put msg')
                continue
            if msg_obj_b is None or msg_dict_b is None:
                logging.warning(f'Problem to put msg alias_msg = {alias_msg}')
                continue
            self._redis.hset(alias_msg, 'dict_id', pickle.dumps(dict_id))
            if isinstance(msg_id, list):
                self._redis.hset(alias_msg, 'msg_id_list', pickle.dumps(msg_id))
                for vv in msg_id:
                    aliases_dict[vv] = alias_msg
            elif msg_id is not None:
                self._redis.hset(alias_msg, 'msg_id', msg_id)
                aliases_dict[msg_id] = alias_msg
            self._redis.hset(alias_msg, 'msg_obj', msg_obj_b)
            self._redis.hset(alias_msg, 'msg_dict', msg_dict_b)
        self._redis.hset(item_id, 'common_dict', common_dict_b)
        self._redis.hset(item_id, 'aliases_dict', pickle.dumps(aliases_dict))

        # Освобождаем лок
        unlock(local_lock)


class SenderMsgRedis(SenderMsg):
    """ Класс объекта отвечающего за отправку сообщений.
        Обработка очереди сообщений, выполняется в отдельном потоке одного (только Одного!!!)
        из процессов-работников программы """

    MAIN_QUEUE_LOCK_NAME = 'main_queue_lock_mod'
    LOCAL_QUEUE_LOCK_NAME = 'local_queue_lock_mod'
    KEY_QUEUE = 'queue'
    KEY_ERROR_QUEUE = 'err_queue'
    _thread = None

    is_connection_ok = True

    def __init__(self, sender=None, msg_getter=None, redis_obj=None, redis_host='localhost', redis_port=6379,
                 redis_db=1,
                 time_to_sleep=0.5, timeout_main_lock=None,
                 time_begin=0., time_end=5., redis_username=None, redis_password=None,
                 max_attempt_to_send=3, time_to_sleep_with_error=None):
        """
        :param sender - объект-функция реальной отправки сообщения
        :param msg_getter - объект потомок MsgGetter
        :param redis_host - параметр для подключения к redis
        :param redis_port - параметр для подключения к redis
        :param redis_db - параметр для подключения к redis (номер базы данных, где хранится очередь)
        :param time_to_sleep - длительность паузы между отправками сообщений
        :param time_to_sleep_with_error- длительность паузы между отправками сообщений при неудачной отправке
        :param timeout_main_lock - время, на которое блокируется БД redis, в которой хранится очередь
        :param time_begin - начало интервала, из которого берется значения случайного интервала запуска обработчика
        :param time_end  - конец интервала, из которого берется значения случайного интервала запуска обработчика
        Случайное значение необходимо, чтобы исключить конфликт за захват главного лока, поскольку обработку очереди
        должен обеспечить только один SenderMsg
        :param max_attempt_to_send - максимальное число попыток отправки
        """
        assert isinstance(msg_getter, MsgGetter) or msg_getter is None
        assert isinstance(sender, AbstractSender) or sender is None
        self._msg_getter = msg_getter

        self._sender = sender
        self._time_to_sleep = time_to_sleep
        if time_to_sleep_with_error is not None:
            self._time_to_sleep_with_error = time_to_sleep_with_error
        else:
            self._time_to_sleep_with_error = 2 * self._time_to_sleep
        self._max_attempt_to_send = max_attempt_to_send
        if timeout_main_lock is not None and timeout_main_lock > time_to_sleep:
            self._timeout_main_lock = timeout_main_lock
        else:
            self._timeout_main_lock = time_to_sleep + 1.
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password,
                                  ssl_cert_reqs=None, ssl=True) \
            if redis_obj is None else redis_obj

        self._token = str(uuid.uuid1())
        self._main_lock = self._redis.lock(self.__class__.MAIN_QUEUE_LOCK_NAME,
                                           timeout=self._timeout_main_lock,
                                           blocking_timeout=self._timeout_main_lock,
                                           thread_local=False)

        self._local_token = str(uuid.uuid1())
        self._local_lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME,
                                            timeout=self._timeout_main_lock,
                                            blocking_timeout=self._timeout_main_lock,
                                            thread_local=True)

        self._thread = None
        # self._local_lock = self._redis.lock(self.__class__.LOCAL_QUEUE_LOCK_NAME, thread_local=False)
        # Пытаемся сразу запустить handle_function.
        # Для избежания конфликтов, делаем это в случаное время
        sleep(uniform(time_begin, time_end))

        try:
            tt = self._main_lock.locked()
        except Exception as e:
            self.is_connection_ok = False
            tt = True
            logging.error(e)

        if not tt:
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
                except Exception as e:
                    pass
                return
        # if not self._main_lock.locked():

        # Как только освободился сразу захватываем главный лок со своим токеном
        self._main_lock.acquire(blocking=True, token=self._token)

        # обработка очереди
        try:
            time_to_sleep = self._handle_queue()
        except:
            self.stop_handle()
            return

        # освобождаем локальный лок
        try:
            self._local_lock.do_release(self._local_token)
        except Exception:
            pass

        # try:
        #     self._thread.cancel()
        # except Exception:
        #     pass
        self._thread = Timer(time_to_sleep, self.handle_function)
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
        time_to_sleep = self._time_to_sleep
        if self._msg_getter is None or self._sender is None:
            return time_to_sleep
        serialized_v = self._redis.rpop(self.__class__.KEY_QUEUE)
        if serialized_v is not None:
            v = pickle.loads(serialized_v)
            dict_id = v.get('dict_id', None)
            alias_msg = v.get('alias_msg', None)
            is_new_msg_ = v.get('is_new_msg', False)
            time_to_sleep = v.get('time_to_sleep', time_to_sleep)  # служебный параметр для отправщика сообщений
            # показывает какую паузу задать
            attempt_to_send = v.get('attempt_to_send', 1)
            flag_to_resend_msg = False  # Флаг переотправки сообщения

            msg_id = None
            state = 0

            try:
                msg_id, state = self._sender(dict_id, v['msg_id'], v['msg_obj'])
            except KeyError:
                pass
            except SenderError as e:
                if e.seconds >= 0:
                    v['time_to_sleep'] = time_to_sleep = self._time_to_sleep_with_error
                    if attempt_to_send < self._max_attempt_to_send:
                        v['attempt_to_send'] = attempt_to_send + 1
                        self._redis.lpush(self.__class__.KEY_QUEUE, pickle.dumps(v))
                        flag_to_resend_msg = True
            except Exception as e:
                logging.error(f' -2 SenderError e = {e}, dict_id={dict_id}, '
                              f'msg_id={v["msg_id"]}, msg_obj={v["msg_obj"]}')
                pass
            finally:
                # Метода send_msg в любом случае должен вызываться, т.к. от увеличивает семафор
                # Если конечно это первая попытка отправки сообщения
                if not flag_to_resend_msg:
                    self._msg_getter.send_msg(
                        alias_msg=alias_msg,
                        dict_id=dict_id,
                        msg_id=msg_id if msg_id is not None else v.get('msg_id', None),
                        state=state,
                        is_new_msg=is_new_msg_
                    )
        return time_to_sleep

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
            # msg_id = msg_context_obj.get('msg_id', None)

            # Используется отдельный флаг для индикации нового сообщения (флаг is_new_msg), поскольку
            # msg_id==None - это не совсем надежный способ, т к сообщение может не успеть
            # отправиться и находится в очереди сендра, но все данные по нему уже записались
            is_new_msg_ = msg_context_obj.get('is_new_msg', False)
            # dict_id = msg_context_obj.get('dict_id', None)
            # alias_msg = msg_context_obj.get('alias_msg', None)
            if msg_context_obj.get('is_msg_changed', True) or is_new_msg_:
                # Выявляем удаление сообщения (msg_obj == None или == dict())
                msg_obj = msg_context_obj.get('msg_obj', None)
                flag_ = msg_obj is None or len(msg_obj) == 0
                if flag_:
                    msg_context_obj['common_dict'] = dict()
                    msg_context_obj['msg_dict'] = dict()

                if not (flag_ and is_new_msg_):
                    # Отправляем, если только сообщение не идет на удаление и при этом еще новое (неотправлено)
                    self._msg_getter.set_msg(msg_context_obj)
                    self._redis.lpush(self.__class__.KEY_QUEUE, pickle.dumps(msg_context_obj))
            else:
                # Вызываем set_msg и send_msg в любом случае,
                # чтоб уменьшить семафор и снять лок
                self._msg_getter.set_msg(msg_context_obj)
                # self._msg_getter.send_msg(alias_msg=alias_msg, dict_id=dict_id, msg_id=msg_id)

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
    SUF_REMOTE_INSTRUCTIONS = b"_r_ins"

    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0,
                 timeout=300, blocking_timeout=310, redis_username=None, redis_password=None,
                 redis_obj=None):
        """ Если какая-то команда отсутствует, она может быть взята из common_dict_def """
        # self._redis_host = redis_host
        # self._redis_port = redis_port
        # self._redis_db = redis_db
        self._timeout_ = timeout
        self._blocking_timeout = blocking_timeout
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password,
                                  ssl_cert_reqs=None, ssl=True) \
            if redis_obj is None else redis_obj

    def _get_remote_instructions(self, context: ContextItem, remote_instructions_name: str) -> ContextItem:
        temp = self._redis.lrange(remote_instructions_name, 0, -1)
        r_instructions = context.remote_instructions
        if temp is not None:
            for v in temp:
                try:
                    cur_ = pickle.loads(v)
                except:
                    continue
                """
                Формат cur_:
                (<имя DataItem>, <Список изменений таблицы элемента DataItem>)

                <Список изменений таблицы элемента DataItem>:
                    [('ins', value), ('del', value), ...]
                """
                try:
                    item_: list = r_instructions[cur_[0]]
                except KeyError:
                    item_ = list()
                item_.extend(cur_[1])
                r_instructions[cur_[0]] = item_
            context.remote_instructions = r_instructions
        return context

    def get_context(self, dict_id) -> ContextItem:
        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self.__class__.SUF_STR_CONTENT_STORAGE
        remote_instructions_name = item_id + self.__class__.SUF_REMOTE_INSTRUCTIONS

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)

        if lock.acquire(blocking=True, token=token):
            temp = self._redis.hget(item_id, "context")
            context = pickle.loads(temp) if temp is not None else ContextItem()

            # Для совместимости со старыми версиями
            if not isinstance(context, ContextItem):
                if isinstance(context, dict):
                    context = ContextItem(
                        session=context.get('session', None),
                        corr_aliases=context.get('corr_aliases', None),
                        remote_instructions=context.get('remote_instructions', None)
                    )
                else:
                    context = ContextItem()

            context = self._get_remote_instructions(context, str(remote_instructions_name))
        else:
            raise IsContextProcessError("This context is busy {}".format(dict_id))
        return context

    # def get_context_new(self, dict_id):
    #     token = get_hash(dict_id)
    #     item_id = str(token).encode()
    #
    #     temp = self._redis.hget(item_id, "context")
    #     context = pickle.loads(temp) if temp is not None else ContextItem()
    #
    #     return context

    def set_context(self, dict_id, context=None):
        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self.__class__.SUF_STR_CONTENT_STORAGE
        remote_instructions_name = item_id + self.__class__.SUF_REMOTE_INSTRUCTIONS

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)

        # if not lock.locked():
        #     raise IsContextProcessError("Already unlocked {}".format(dict_id))

        if context is not None:
            self._redis.hset(item_id, "context", pickle.dumps(context))
            try:
                self._redis.delete(remote_instructions_name)
            except:
                pass

        # Снимаем блокировку
        if lock.locked():
            lock.do_release(token)

    # def set_context_new(self, dict_id, context=None):
    #     token = get_hash(dict_id)
    #     item_id = str(token).encode()
    #
    #     if context is not None:
    #         self._redis.hset(item_id, "context", pickle.dumps(context))

    def send_remote_instructions(self, dict_id: dict, di_name: str, instr_list: list):
        """
        Метод отправки удаленной инструкций для элемента данный DataItem в контексте по адресу dict_id
        :param dict_id: идентификатор контекста
        :param di_name: идентификатор DataItem
        :param instr_list: список инструкций изменения главной таблицы в DataItem
            например ([('ins', value1), ('del', value2)])
        :return:
        """
        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self.__class__.SUF_STR_CONTENT_STORAGE
        remote_instructions_name = item_id + self.__class__.SUF_REMOTE_INSTRUCTIONS

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)

        if lock.acquire(blocking=True, token=token):
            self._redis.lpush(remote_instructions_name, pickle.dumps((di_name, instr_list)))
        else:
            raise IsContextProcessError("This context is busy {}".format(dict_id))

    def get_data(self, dict_id):
        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self.__class__.SUF_STR_CONTENT_STORAGE
        remote_instructions_name = item_id + self.__class__.SUF_REMOTE_INSTRUCTIONS

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)

        if lock.acquire(blocking=True):
            temp = self._redis.hget(item_id, "context")
            context = pickle.loads(temp) if temp is not None else ContextItem()
            context = self._get_remote_instructions(context, str(remote_instructions_name))
            context_b = pickle.dumps(context)

            self._redis.hdel(item_id, 'context')
            try:
                self._redis.delete(remote_instructions_name)
            except:
                pass
            if int(self._redis.hlen(item_id)) == 0:
                self._redis.delete(item_id)
            unlock(lock)
            return context_b

    def put_data(self, dict_id, context_b):
        token = get_hash(dict_id)
        item_id = str(token).encode()
        lock_name = item_id + self.__class__.SUF_STR_CONTENT_STORAGE

        lock = self._redis.lock(lock_name,
                                timeout=self._timeout_,
                                blocking_timeout=self._blocking_timeout,
                                thread_local=False)

        if lock.acquire(blocking=True):
            if context_b is not None:
                self._redis.hset(item_id, "context", context_b)
            lock.release()


# TODO: Передать в GetterReplicatorPostgreSQL функционал ContextStorageRedis,
#  для корректной синхронизации с PostgreSQL
class GetterReplicatorPostgreSQL(MsgGetter, ContextStorage):
    _SORTED_SET_NAME = 'torch_id_set'
    _LOCK_REPLICATOR_NAME = b'lock_repl'
    _LOCK_PG = b'pg_lock'
    _common_dict_def = dict()

    def __init__(self, getter: MsgGetterRedis, context_storage: ContextStorageRedis,
                 redis_storage, pg_param: dict, ttl: int, timeout=300, blocking_timeout=310):
        """
        :param context_storage - хранилище контекста
        :param ttl: - время жизни объекта в redis
        :param redis_storage: - хранилище очереди
        :param pg_param: - параметры подключения к postgresql

        """
        super().__init__(common_dict_def=self._common_dict_def)
        self._getter = getter
        self._context_storage = context_storage
        self._redis = redis_storage
        self._hostname = pg_param['hostname']
        self._username = pg_param['username']
        self._password = pg_param['password']
        self._database = pg_param['database']
        self._port = pg_param['port']
        self._ttl = ttl
        self._timeout_ = timeout
        self._blocking_timeout = blocking_timeout
        self.create_tabs()

    def add_command_def(self, command, vertex_name, control_dict, to_common_dict=True):
        return self._getter.add_command_def(command, vertex_name, control_dict, to_common_dict)

    def add_filter(self, filter_getter, command, vertex_name, control_dict):
        return self._getter.add_filter(filter_getter, command, vertex_name, control_dict)

    def _get_db_connect(self):
        return psycopg2.connect(host=self._hostname, user=self._username,
                                password=self._password, dbname=self._database, port=self._port)

    def create_tabs(self):
        """ Создание необходимых таблиц """
        # try:
        with self._get_db_connect() as conn:
            cur = conn.cursor()
            request = """CREATE TABLE if not exists tab(
                                dict_id_uuid uuid PRIMARY KEY,
                                data bytea NOT NULL,
                                aliases_array uuid[],
                                time_of_backup NUMERIC(16, 5) DEFAULT date_part('epoch', now())
                            );"""
            cur.execute(request)
        # except Exception as e:
        #     logging.error('Problem with DB: %s' % str(e))
        #     raise ContextError('Problem with DB')

    def get_lock_name(self, dict_id, suff):
        token = get_hash(dict_id)
        item_id = str(token).encode()
        return f'{item_id}_{suff}'

    def get_data_by_alias(self, alias_msg):
        try:
            with self._get_db_connect() as conn:
                cur = conn.cursor()
                request = """SELECT data FROM tab WHERE (%s)::uuid=ANY(aliases_array);"""
                cur.execute(request, (alias_msg,))
                try:
                    res = cur.fetchone()
                    if res is not None:
                        return pickle.loads(res[0])
                    else:
                        return None
                except:
                    return None
        except Exception as e:
            return None

    def get_data(self, dict_id):
        """
        Возвращает данные из постоянного хранилища PostgreSQL в redis
        :param dict_id:
        :return:
        """
        try:
            with self._get_db_connect() as conn:
                cur = conn.cursor()
                request = """SELECT data FROM tab WHERE dict_id_uuid=md5(%s)::uuid;"""
                cur.execute(request, (get_str(dict_id),))
                try:
                    res = cur.fetchone()
                    if res is not None:
                        return pickle.loads(res[0])
                    else:
                        return None
                except:
                    return None
        except Exception as e:
            return None

    def set_data(self, dict_id, value):
        """
        Сохраняет данные в постоянное хранилище PostgreSQL из redis
        :param dict_id: ключ
        :param value: архивируемое значение
        :return:
        """
        try:
            with self._get_db_connect() as conn:
                cur = conn.cursor()
                request = """INSERT INTO tab (dict_id_uuid, data, aliases_array) 
                                VALUES (md5(%(dict_id_str)s)::uuid, %(data)s, %(aliases_array)s::uuid[])
                                ON CONFLICT (dict_id_uuid) DO UPDATE SET data=%(data)s, 
                                            time_of_backup=DEFAULT, 
                                            aliases_array=(%(aliases_array)s::uuid[]);"""
                cur.execute(request,
                            dict(dict_id_str=get_str(dict_id),
                                 data=pickle.dumps(value),
                                 aliases_array=[v['alias_msg'] for v in value['msg_list']])
                            )
        except Exception as e:
            logging.warning('Ошибка переноса из redis в базу: ', e)
            return False
        return True

    def torch(self, dict_id):
        """ Метод записывает время последнего использования данных по идентификатору dict_id """
        self._redis.zadd(self._SORTED_SET_NAME, {pickle.dumps(dict_id): int(time.time())})

    def handle_of_set(self):
        """ Обработка множества сообщений на возможность переноса в постоянное хранилище"""
        # Получаем все элементы нуждающие в переносе
        my_lock = self._redis.lock(self._LOCK_REPLICATOR_NAME,
                                   timeout=self._timeout_,
                                   blocking_timeout=self._blocking_timeout,
                                   thread_local=False)
        if my_lock.acquire(blocking=False):
            s_list = self._redis.zrangebyscore(self._SORTED_SET_NAME, 0, int(time.time()) - self._ttl)
            for v in s_list:
                dict_id = pickle.loads(v)
                my_lock_dict_id = self._redis.lock(self.get_lock_name(dict_id, self._LOCK_PG),
                                                   timeout=self._timeout_,
                                                   blocking_timeout=self._blocking_timeout,
                                                   thread_local=False)
                if my_lock_dict_id.acquire(blocking=True):
                    try:
                        data = self._getter.get_data(dict_id)
                    except IsMsgProcessError as e:
                        logging.info('Lock by dict_id={} is busy'.format(dict_id))
                        continue
                    else:
                        try:
                            data['context'] = self._context_storage.get_data(dict_id)
                            data['dict_id'] = dict_id

                            token = get_hash(dict_id)
                            item_id = str(token).encode()

                            if self.set_data(dict_id=dict_id, value=data):
                                # Удаляем заархивированный элемент
                                self._redis.zrem(self._SORTED_SET_NAME, v)
                        except Exception as e:
                            pass
                    finally:
                        my_lock_dict_id.release()
            my_lock.release()
        else:
            logging.info('Lock of replicator is busy')

    def getter_wrapper(self, func):
        """ Декоратор для получения данных из хранилища """

        def wrapper(*args, **kwargs):
            arg_dict = get_arg_dict(func, args, kwargs)
            dict_id = arg_dict.get('dict_id', None)
            alias_msg = arg_dict.get('alias_msg', None)

            # print(f' --- Test wrapper func={func.__name__}')

            if alias_msg is not None and not self._getter.exists_alias_msg(alias_msg):
                # print(f' --- --- alias_msg={alias_msg}')
                my_lock_alias_msg = self._redis.lock(f'{alias_msg}_{self._LOCK_PG}',
                                                     timeout=self._timeout_,
                                                     blocking_timeout=self._blocking_timeout,
                                                     thread_local=False)
                if my_lock_alias_msg.acquire(blocking=True):
                    try:
                        data = self.get_data_by_alias(alias_msg)
                        if data is not None:
                            # print('Пришли сюда 1')
                            dict_id_a = data['dict_id']
                            my_lock_dict_id = self._redis.lock(self.get_lock_name(dict_id_a, self._LOCK_PG),
                                                               timeout=self._timeout_,
                                                               blocking_timeout=self._blocking_timeout,
                                                               thread_local=False)
                            if my_lock_dict_id.acquire(blocking=True):
                                # print('Пришли сюда 2')
                                try:
                                    if not self._getter.exists_dict_id(dict_id_a):
                                        # print(f'Пришли сюда 3 item_id={str(get_hash(dict_id_a)).encode()}')
                                        self._getter.put_data(dict_id=dict_id_a, value=data)
                                        # Если dict_id_a!=dict_id то исходя из логики работы
                                        # self._context_storage для dict_id_a скорее всего не понадобится,
                                        # но для целостности тоже выгрузим их
                                        self._context_storage.put_data(
                                            dict_id=dict_id_a,
                                            context_b=data['context'])
                                finally:
                                    my_lock_dict_id.release()
                    finally:
                        my_lock_alias_msg.release()
            elif dict_id is not None and not self._getter.exists_dict_id(dict_id):
                my_lock_dict_id = self._redis.lock(self.get_lock_name(dict_id, self._LOCK_PG),
                                                   timeout=self._timeout_,
                                                   blocking_timeout=self._blocking_timeout,
                                                   thread_local=False)
                if my_lock_dict_id.acquire(blocking=True):
                    try:
                        data = self.get_data(dict_id)
                        if data is not None:
                            self._getter.put_data(dict_id=dict_id, value=data)
                            self._context_storage.put_data(dict_id=dict_id,
                                                           context_b=data['context'])
                    finally:
                        my_lock_dict_id.release()

            res = func(*args, **kwargs)

            return res

        return wrapper

    ### Переопределенные методы MsgGetter

    def acquire_main_lock(self, dict_id):
        """ Захватывает главный лок """
        # MsgGetterError
        # NoSuchMsgError
        # return self._getter.acquire_main_lock(dict_id)
        func = self.getter_wrapper(self._getter.acquire_main_lock)
        return func(dict_id)

    def release_main_lock(self, dict_id):
        """ Освобождает главный лок """
        self._getter.release_main_lock(dict_id)
        # Обрабатываем очередь сеансов на необходимость переноса в postgresql
        self.handle_of_set()

    def get_dict_id_by_alias(self, alias_msg):
        """ Получение dict_id и msg_id по значению алиаса """
        func = self.getter_wrapper(self._getter.get_dict_id_by_alias)
        return func(alias_msg)

    def get_msg_by_alias(self, alias_msg, dict_id, command=None):
        """ Возвращает управляющие данные по alias_msg """
        func = self.getter_wrapper(self._getter.get_msg_by_alias)
        return func(alias_msg, dict_id, command)

    def get_msg(self, dict_id, msg_id, command):
        """ Метод получает сообщение по msg_id """
        func = self.getter_wrapper(self._getter.get_msg)
        res = func(dict_id, msg_id, command)
        return res

    def send_msg(self, alias_msg, dict_id, msg_id, is_sending_err=False, state=0, is_new_msg=True):
        """ Метод вызвается при физической отправке сообщения,
            когда становится известным его msg_id,
            если msg_id==None, то значит сообщение было удалено
            is_sending_err - флаг, обозначающий ошибки при отправке,
            в этом случае msg_id тоже == None"""
        # return self._getter.send_msg(alias_msg, dict_id, msg_id, is_sending_err=is_sending_err,
        #                              state=state, is_new_msg=is_new_msg)
        if is_new_msg:
            func = self.getter_wrapper(self._getter.send_msg)
            func(alias_msg, dict_id, msg_id, is_sending_err=is_sending_err,
                 state=state, is_new_msg=is_new_msg)

    def set_msg(self, dict_msg_arg):
        """ Устанавливает данные сообщения еще до его физической отправки
            dict_msg_arg = dict(dict_id=dict_id, msg_id=msg_id_new,
                                msg_obj=msg_obj, common_dict=common_dict, msg_dict=msg_dict) """
        # Перед тем как записать сообщение, необходимо убедиться,
        # что все данные загружены в Redis из БД и при необходимости
        # подгрузить их, это сделано на случай,
        # если вдруг сообщение отправляется по какому-то новому dict_id
        dict_id = dict_msg_arg['dict_id']

        func = self.getter_wrapper(lambda dict_id: True)
        func(dict_id)

        try:
            self.torch(dict_id=dict_id)
        except KeyError:
            pass
        return self._getter.set_msg(dict_msg_arg)

    def get_context(self, dict_id):
        """ Получить контекст из хранилища либо выдает dict(), если нету"""
        func = self.getter_wrapper(self._context_storage.get_context)
        return func(dict_id)

    def set_context(self, dict_id, context=None):
        try:
            self.torch(dict_id=dict_id)
        except KeyError:
            pass
        return self._context_storage.set_context(dict_id, context)

    def send_remote_instructions(self, dict_id: dict, di_name: str, instr_list: list):
        """
        Метод отправки удаленной инструкций для элемента данный DataItem в контексте по адресу dict_id
        :param dict_id: идентификатор контекста
        :param di_name: идентификатор DataItem
        :param instr_list: список инструкций изменения главной таблицы в DataItem
            например ([('ins', value1), ('del', value2)])
        :return:
        """
        func = self.getter_wrapper(lambda dict_id: True)
        func(dict_id)

        try:
            self.torch(dict_id=dict_id)
        except KeyError:
            pass
        return self._context_storage.send_remote_instructions(dict_id, di_name, instr_list)


class MediaGroupHandler:
    """ Обработчик групповых медиа сообщений
    """
    _media_suf = '_media_group'
    _lock_name_suf = '_media_group_handler'

    def __init__(self, redis_obj=None, redis_host='localhost', redis_port=6379,
                 redis_db=1,
                 time_to_sleep=0.5, timeout_lock=30,
                 redis_username=None, redis_password=None):
        self._time_to_sleep = time_to_sleep
        if timeout_lock is not None and timeout_lock > time_to_sleep:
            self._timeout_lock = timeout_lock
        else:
            self._timeout_lock = time_to_sleep + 1.
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                  username=redis_username, password=redis_password,
                                  ssl_cert_reqs=None, ssl=True) \
            if redis_obj is None else redis_obj

    def __call__(self, media_group_id, item, callback_fun):
        lock_name = str(media_group_id) + self._lock_name_suf
        media_name = str(media_group_id) + self._media_suf
        _lock = self._redis.lock(lock_name,
                                 timeout=self._timeout_lock,
                                 blocking_timeout=self._timeout_lock,
                                 thread_local=False)
        if _lock.acquire(blocking=False, token=lock_name):
            self._redis.delete(media_name)
            _thread = Timer(self._time_to_sleep,
                            lambda: self.handler_function(media_group_id, callback_fun))
            _thread.start()
        self._redis.lpush(media_name, pickle.dumps(item))

    def handler_function(self, media_group_id, callback_fun):
        lock_name = str(media_group_id) + self._lock_name_suf
        media_name = str(media_group_id) + self._media_suf
        _lock = self._redis.lock(lock_name,
                                 timeout=self._timeout_lock,
                                 blocking_timeout=self._timeout_lock,
                                 thread_local=False)
        group_result = list()
        while True:
            item_b = self._redis.rpop(media_name)
            if item_b is None:
                break
            try:
                group_result.append(pickle.loads(item_b))
            except:
                continue

        callback_fun(group_result)

        self._redis.delete(media_name)
        try:
            _lock.do_release(expected_token=lock_name)
        except:
            pass


# if __name__ == "__main__":
logging.basicConfig(format=u'  -@- %(levelname)-8s [%(asctime)s] %(message)s', level=logging.WARNING)
