#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

import psycopg2
from dialog_machine.dialog_machine_core import *
import pickle

from dialog_machine.interfaces import ContextStorage


class ContextStoreHeroku(ContextStorage):

    @contract(param='dict')
    def __init__(self, param):
        try:
            self._hostname = param['hostname']
            self._username = param['username']
            self._password = param['password']
            self._database = param['database']
        except KeyError as e:
            logging.error('Problem with DB connecting parametres: %s' % str(e))
            raise ContextError('Problem with DB connecting parametres')
        self._lock_set = set()

    def _tuple_ord(self, d):
        """ Получаем упорядоченный tuple """
        t = list(d.keys())
        t.sort()
        return tuple([d[k] for k in t])

    def _get_db_connect(self):
        return psycopg2.connect(host=self._hostname, user=self._username,
                                password=self._password, dbname=self._database)

    @contract(dict_id='dict_id_contract', returns='context_contract|None')
    def get_context(self, dict_id):
        """ Получить контекст работы с пользователем
            (если не найден, вернуть None)
        """
        try:
            with self._get_db_connect() as conn:
                cur = conn.cursor()
                request = """SELECT data FROM global_state WHERE id_author=%(user_id)s AND id_chat=%(chat_id)s;"""
                cur.execute(request, dict_id)
                try:
                    res = cur.fetchone()
                    if res:
                        my_data = res[0]
                    else:
                        return None
                except IndexError as e:
                    return None
        except Exception as e:
            logging.error('Problem with DB: %s' % str(e))
            raise ContextError('Problem with DB')

        try:
            us_data = pickle.loads(my_data)
        except Exception as e:
            logging.error('Problem with pickle in _get_context: %s' % str(e))
            raise ContextError('Problem with pickle in _get_context')
        else:
            temp_id = self._tuple_ord(dict_id)
            if not temp_id in self._lock_set:
                self._lock_set.add(temp_id)
            else:
                raise IsContextProcessError('Context is Process')
            return us_data

    @contract(dict_id='dict', context='context_contract')
    def set_context(self, dict_id, context=None):
        """ Сохранить контекст работы с пользователем """
        if not context is None:
            try:
                my_data = psycopg2.Binary(pickle.dumps(context))
            except Exception as e:
                logging.error('Problem with pickle in set_context: %s' % str(e))
                raise ContextError('Problem with pickle in set_context')

            try:
                with self._get_db_connect() as conn:
                    cur = conn.cursor()
                    temp_dict = {'user_id': dict_id['user_id'], 'chat_id': dict_id['chat_id'],
                                 'bot_name': dict_id['bot_name'],
                                 'data': my_data}
                    cur.execute("""INSERT INTO global_state (id_author, id_chat, data, TimeUpdate)
                                       VALUES (%(user_id)s, %(chat_id)s, %(data)s, date_part('epoch', now()))
                                       ON CONFLICT (id_author, id_chat) DO UPDATE SET data=EXCLUDED.data, 
                                            TimeUpdate=date_part('epoch', now());""",
                                temp_dict)
            except Exception as e:
                logging.error('Problem with DB in set_context: %s' % str(e))
                raise ContextError('Problem with DB in set_context ')
            else:
                temp_id = self._tuple_ord(dict_id)
        self._lock_set.discard(temp_id)
