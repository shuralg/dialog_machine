from abc import ABC, abstractmethod


class ContextItem:
    """ Единица обмена данными контекста.
        Содержит
    """

    def __init__(self, session=None, corr_aliases=None, remote_instructions=None):
        """
        :param session: Набор элементов DataItem (потомков DataItemInterface). Словарь.
        :param corr_aliases: Соответствия алиасов и имен соответствующих им DataItem. Словарь.
        :param remote_instructions: Инструкции от удаленных контекстов к данному. Словарь списков
        """
        if session is None:
            self.session = dict()
        else:
            assert isinstance(session, dict)
            self.session = session

        if corr_aliases is None:
            self.corr_aliases = dict()
        else:
            assert isinstance(corr_aliases, dict)
            self.corr_aliases = corr_aliases

        if remote_instructions is None:
            self.remote_instructions = dict()
        else:
            assert isinstance(remote_instructions, dict)
            self.remote_instructions = remote_instructions



class ContextStorage(ABC):
    """ Класс физического хранилища контекстов. Хранилище может быть организовано
        разными способами: хранение в базе, в файле... Данный класс определяет общий интерфейс
        к различным реализациям"""

    @abstractmethod
    def get_context(self, dict_id) -> ContextItem:
        """ Получить контекст из хранилища либо выдает dict(), если нету"""
        return ContextItem()

    @abstractmethod
    def set_context(self, dict_id, context=None):
        pass

    @abstractmethod
    def send_remote_instructions(self, dict_id: dict, di_name: str, instr_list: list):
        """
        Метод отправки удаленной инструкций для элемента данный DataItem в контексте по адресу dict_id
        :param dict_id: идентификатор контекста
        :param di_name: идентификатор DataItem
        :param instr_list: список инструкций изменения главной таблицы в DataItem
            например ([('ins', value1), ('del', value2)])
        :return:
        """
        pass


class MsgGetter(ABC):
    """ Класс объекта хранилища данных сообщений и контекста этих сообщений. Прототип """

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
        self._msg_dict_def = dict()
        self._common_command_dict = dict()

    def add_command_def(self, command, vertex_name, control_dict, to_common_dict=True):
        """
        Добавление команды по-умолчанию в словарь common_dict_def
        :param command: добавляемая команда
        :param vertex_name: имя вершины
        :param control_dict: управляющие данные
        :param to_common_dict: флаг, показывающий, что добавление команды по умолчанию будет в common_dict
                                иначе в msg_dict_def
        :return: None
        """
        if to_common_dict:
            self._common_dict_def[command] = dict(vertex_name=vertex_name, control_dict=control_dict)
        else:
            self._msg_dict_def[command] = dict(vertex_name=vertex_name, control_dict=control_dict)

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
            except KeyError as e:
                raise KeyError(e)
                # if command is None:
                #     raise NoneCommandError()
                # else:
                #     raise NoMatchCommandError()
        else:
            for v in temp_list:
                filter_getter = v['filter']
                if filter_getter(dict_id):
                    return dict(vertex_name=v['vertex_name'], control_dict=v['control_dict'])
            try:
                return self._common_dict_def[command]
            except KeyError as e:
                raise KeyError(e)
                # if command is None:
                #     raise NoneCommandError()
                # else:
                #     raise NoMatchCommandError()

    def get_control_msg_(self, dict_id, command):
        """ Внутренний метод для получения значений vertex_name и control_dict по умолчанию,
            если соответствующих значений не находит. то выбрасывает KeyError"""
        try:
            return self._msg_dict_def[command]
        except KeyError as e:
            # if command is None:
            #     raise NoneCommandError()
            # else:
            #     raise NoMatchCommandError()
            raise KeyError(e)

    @abstractmethod
    def acquire_main_lock(self, dict_id):
        """ Захватывает главный лок, возвращает алиас сообщения по-умолчанию в этом контексте"""
        pass

    @abstractmethod
    def release_main_lock(self, dict_id):
        """ Освобождает главный лок """
        pass

    @abstractmethod
    def get_dict_id_by_alias(self, alias_msg):
        """ Получение dict_id и msg_id по значению алиаса """
        return None

    @abstractmethod
    def get_msg_by_alias(self, alias_msg, dict_id, command=None):
        """ Возвращает управляющие данные по alias_msg """
        return dict(vertex_name=None, control_dict=dict()), None
        # dict(dict_id=dict_id, msg_id=msg_id, msg_obj=None, common_dict=dict(), msg_dict=dict(),
        #      is_msg_changed=False, is_msg_dict=False)

    @abstractmethod
    def get_msg(self, dict_id, msg_id, command):
        """ Метод получает сообщение по msg_id """
        pass

    @abstractmethod
    def send_msg(self, alias_msg, dict_id, msg_id, is_sending_err=False, state=0, is_new_msg=True):
        """ Метод вызвается при физической отправке сообщения,
            когда становится известным его msg_id,
            если msg_id==None, то значит сообщение было удалено
            :param is_sending_err: флаг, обозначающий ошибки при отправке,
                            в этом случае msg_id тоже == None
            :param state: состояние отправленно сообщения
                            (0 - ничего не сделано с сообщением
                             1 - добавлено сообщение
                             2 - изменено сообщение
                             3 - сообщение удалено)
            :param msg_id: полученное id сообщения
            :param dict_id:
            :param alias_msg: алиас сообщения
            :param is_new_msg: True если сообщение новое, False - если сообщение измененное"""
        pass

    @abstractmethod
    def set_msg(self, dict_msg_arg):
        """ Устанавливает данные сообщения еще до его физической отправки
            dict_msg_arg = dict(dict_id=dict_id, msg_id=msg_id_new,
                                msg_obj=msg_obj, common_dict=common_dict, msg_dict=msg_dict) """
        pass


class SenderMsg(ABC):
    """ Класс-прототип объекта, отвечающего за отправку сообщения, содержания очереди отправки и получение для
            сообщений msg_id"""
    _msg_getter = None
    _sender = None

    def set_msg_getter(self, msg_getter):
        """ задает msg_getter  на этапе выполнения """
        assert isinstance(msg_getter, MsgGetter)
        self._msg_getter = msg_getter

    def set_sender(self, sender):
        """ задает sender  на этапе выполнения """
        assert isinstance(sender, AbstractSender)
        self._sender = sender

    @abstractmethod
    def __call__(self, msg_context_obj):
        """
            msg_context_obj = dict(dict_id, msg_id, msg_obj, common_dict, msg_dict)
        """
        return


class AbstractSender(ABC):
    """ Прототип класса физической отправки сообщения """

    @abstractmethod
    def __call__(self, dict_id, msg_id, msg_obj):
        """ Возвращает Номер сообщение (msg_id), и статус модификации сообщения
            (0 - ничего не сделано с сообщением
             1 - добавлено сообщение
             2 - изменено сообщение
             3 - сообщение удалено) """
        return None, 0


class DataItemInterface(ABC):
    """ Класс объекта, который создает интерфейс доступа к контексту,
        Повторяет функционал DataItem, но для всей иерархии объектов DataItem
    """

    # Интерфейс для модели
    @abstractmethod
    def ins_upd_in_table(self, value: dict):
        pass

    @abstractmethod
    def del_in_table(self, value):
        pass

    @abstractmethod
    def clear_table(self):
        pass

    # Интерфейс для доступа к main_dict
    @property
    def cursor(self):
        return None

    @cursor.setter
    def cursor(self, value):
        pass

    @property
    def main_dict(self):
        return dict()

    @main_dict.setter
    def main_dict(self, value: dict):
        pass

    @property
    def table(self):
        return None

    def __getitem__(self, key):
        return None

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    @property
    def key_hierarchy(self):
        return None

    @property
    def key_set(self):
        pass

    @key_set.setter
    def key_set(self, value):
        pass

    @abstractmethod
    def embed(self):
        pass

    @property
    def is_embedding(self):
        return False


