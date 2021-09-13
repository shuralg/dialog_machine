#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

from dialog_machine.dialog_machine_core import *

from time import sleep

from dialog_machine.interfaces import ContextStorage, SenderMsg


class MsgGetterSimple(MsgGetterOld):
    """ Класс объекта хранилища данных сообщений и контекста этих сообщений """

    def __init__(self, common_dict_def, *args, **kwargs):
        """ Если какая-то команда отсутствует, она может быть взята из common_dict_def """
        super().__init__(common_dict_def, *args, **kwargs)
        self.main_common_dict = dict()
        self.main_msg_dict = dict()
        self.lock_dict = dict()

    @staticmethod
    def _tuple_ord(d):
        """ Получаем упорядоченный tuple """
        t = list(d.keys())
        t.sort()
        return tuple([d[k] for k in t])

    def inc_semaphore(self, dict_id):
        pass

    def dec_semaphore(self, dict_id):
        pass

    def get_msg(self, dict_id, msg_id, command):
        tuple_id = self._tuple_ord(dict_id)
        w_common_dict = self.main_common_dict.get(tuple_id, dict())
        w_msg_dict = self.main_msg_dict.get(tuple_id, dict())
        w_lock_set = self.lock_dict.get(tuple_id, set())

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
                    # common_dict_temp = dict()
                    # common_dict_temp[command] = deepcopy(temp_dict)
                    # return dict(dict_id=dict_id, msg_id=None, msg_obj=None, common_dict=common_dict_temp,
                    #             msg_dict=dict())
                    t = deepcopy(temp_dict)
                    return dict(vertex_name=t.get('vertex_name', None),
                                control_dict=t.get('control_dict', dict())), \
                           None
            msg_id_new = temp_dict.get('msg_id', msg_id)
        else:
            msg_id_new = msg_id
            temp_dict = None  # будет обработано дальее когда будут считаны данные сообщения

        # Блокируем сообщение, если еще не заблокировано
        if msg_id_new not in w_lock_set:
            w_lock_set.add(msg_id_new)
            # Сохраняем блокировку
            self.lock_dict[tuple_id] = w_lock_set
        else:
            raise IsMsgProcessError("Message '{}' is locked".format(msg_id_new))

        # Получаем сообщение
        try:
            temp_msg = w_msg_dict[msg_id_new]
            msg_obj = deepcopy(temp_msg.get('msg_obj', None))
            msg_dict = deepcopy(temp_msg['msg_dict'])
        except KeyError:
            # Снимаем блокировку
            w_lock_set.discard(msg_id_new)
            self.lock_dict[tuple_id] = w_lock_set
            raise NoSuchMsgError("No such message had msg_id={}".format(msg_id_new))
        else:
            if temp_dict is None:  # это может быть, если команда была в msg_dict, а не в common_dict
                try:
                    temp_dict = msg_dict[command]
                except KeyError:
                    # Снимаем блокировку
                    w_lock_set.discard(msg_id_new)
                    self.lock_dict[tuple_id] = w_lock_set
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
               dict(dict_id=dict_id, msg_id=msg_id_new, msg_obj=msg_obj, common_dict=common_dict, msg_dict=msg_dict,
                    is_msg_changed=False, is_msg_dict=False)

    def set_msg(self, dict_msg_arg):
        """ dict_msg_arg = dict(dict_id=dict_id, msg_id=msg_id_new,
                                msg_obj=msg_obj, common_dict=common_dict, msg_dict=msg_dict) """
        try:
            dict_id = deepcopy(dict_msg_arg['dict_id'])
            msg_id = deepcopy(dict_msg_arg['msg_id'])
            common_dict = deepcopy(dict_msg_arg['common_dict'])
            msg_dict = deepcopy(dict_msg_arg['msg_dict'])
            msg_obj = deepcopy(dict_msg_arg['msg_obj'])
        except KeyError:
            return

        if msg_id is None:
            return

        # Получаем текущие словари
        tuple_id = self._tuple_ord(dict_id)
        w_common_dict = self.main_common_dict.get(tuple_id, dict())
        w_msg_dict = self.main_msg_dict.get(tuple_id, dict())
        w_lock_set = self.lock_dict.get(tuple_id, set())

        msg_is_new = False
        # Проверяем заблочены ли данные, соответствующие данному сообщению
        if msg_id not in w_lock_set:
            # Если данные не заблокированы и в словаре нет сообщений, сооветствующих данному msg_id
            # то делаем вывод, что это новое сообщение и его надо ввести в систему
            try:
                w_msg_dict[msg_id]
            except KeyError:
                # t = None
                msg_is_new = True
            else:
                return

        # Удаляем из словарей данные, соответствующие этому сообщению, если конечно это сообщение не новое
        if not msg_is_new:
            temp_list = list()
            for k, v in w_common_dict.items():
                try:
                    if v['msg_id'] == msg_id:
                        temp_list.append(k)
                except KeyError:
                    pass
            for k in temp_list:
                del w_common_dict[k]

            try:
                del w_msg_dict[msg_id]
            except KeyError:
                pass

        # Вставляем данные только если есть управляющие данные для обработки
        if len(msg_dict) > 0 or len(common_dict) > 0:
            for k, v in common_dict.items():
                v['msg_id'] = msg_id
                w_common_dict[k] = v

            w_msg_dict[msg_id] = dict(msg_obj=msg_obj, msg_dict=msg_dict)

        # Сохраняем словари
        self.main_common_dict[tuple_id] = w_common_dict
        self.main_msg_dict[tuple_id] = w_msg_dict

        # Разблокируем данные сообщения
        w_lock_set.discard(msg_id)
        self.lock_dict[tuple_id] = w_lock_set


class SenderMsgSimple(SenderMsg):
    """ Класс объекта, отвечающего за отправку сообщения, содержания очереди отправки и получение для
        сообщений msg_id """

    def __init__(self, sender, msg_getter, time_to_sleep):
        """ sender - отвечает за физическую отправку
            msg_getter - хранитель сообщений
            time_to_sleep - пауза миежду сообщениями в секундах"""
        assert isinstance(msg_getter, MsgGetterOld) or msg_getter is None
        assert type(time_to_sleep) is float
        self._sender = sender
        self._msg_getter = msg_getter
        self._time_to_sleep = time_to_sleep
        self._queue = list()

    def __call__(self, msg_context_obj):
        """
            msg_context_obj = dict(dict_id, msg_id, msg_obj, common_dict, msg_dict)
        """
        self._queue.append(deepcopy(msg_context_obj))

    def run(self):
        temp_queue = list()
        while True:
            try:
                v = self._queue.pop(0)
            except IndexError:
                break
            else:
                try:
                    msg_id = self._sender(v['dict_id'], v['msg_id'], v['msg_obj'])
                except Exception:
                    # Если какие-то ошибки при физической отправке сообщения,
                    # откладываем данные этого сообщения во временную очередь, чтоб после окончания цикла
                    # слить ее с исходной
                    temp_queue.append(v)
                else:
                    temp = v
                    temp['msg_id'] = msg_id
                    try:
                        self._msg_getter.set_msg(temp)
                    except Exception:
                        pass
                    else:
                        sleep(self._time_to_sleep)
        self._queue.extend(temp_queue)


class ContextStorageSimple(ContextStorage):

    def __init__(self):
        self._lock_set = set()
        self._main_dict = dict()

    @staticmethod
    def _tuple_ord(d):
        """ Получаем упорядоченный tuple """
        t = list(d.keys())
        t.sort()
        return tuple([d[k] for k in t])

    def get_context(self, dict_id):
        """ Получить контекст из хранилища либо выдает dict(), если нету"""
        tuple_id = self._tuple_ord(dict_id)

        # Проверяем не занят ли контект
        if tuple_id in self._lock_set:
            raise IsContextProcessError("This context is busy {}".format(dict_id))
        else:
            self._lock_set.add(tuple_id)

        return self._main_dict.get(tuple_id, dict())

    def set_context(self, dict_id, context=None):
        # При context=None метод только разблокирует контекст
        tuple_id = self._tuple_ord(dict_id)
        if tuple_id in self._lock_set:
            if context is not None:
                self._main_dict[tuple_id] = deepcopy(context)
            self._lock_set.discard(tuple_id)
