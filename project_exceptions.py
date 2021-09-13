#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

#######################################################
##   Exceptions for classes in dialog_machine_core   ##
#######################################################

class DialogMachineError(Exception):
    pass


class DialogMachineInitError(DialogMachineError):
    pass


class ContextError(DialogMachineError):
    """ Ошибки возвращения/записи контекста """
    pass


class IsContextProcessError(ContextError):
    """ Контекст уже используется """
    pass


class ContextConnectorError(DialogMachineError):
    """ Ошибка ContextConnector """
    pass


class UpdateVarStateError(DialogMachineError):
    pass


class StateMachineError(DialogMachineError):
    """ Ошибки объекта конечного автомата """
    pass


class CheckerError(DialogMachineError):
    """ Ошибки класса-функции, проверяющего вершину """
    pass


class MsgGetterError(DialogMachineError):
    """ Ошибки для MsgGetter """
    pass


class IsMsgProcessError(MsgGetterError):
    """ данное сообщение уже обрабатывается, поэтому заблокировано """
    pass


class NoMatchCommandError(MsgGetterError):
    """ нет соответствующей команды для данного сообщения """
    pass


class NoneCommandError(MsgGetterError):
    """ Команда None (должен быть выдана вершина по умолчанию,
        но этих данных у MsgGetter почему-то нет) """
    pass


class NoSuchMsgError(MsgGetterError):
    """ Нет такого сообщения """
    pass


class SenderError(Exception):
    """ Ошибка физической отправки сообщения, например когда разорвана связь
    или превышен лимит отправки в секунду"""

    def __init__(self, e='', seconds=-1):
        """
        :param seconds: кол-во секунд для паузы между отправкой сообщения
                        при переотправке в случае ошибки, если значение <0,
                        то значит в плучаемое сообщение в случае ошибки переотправлять не нужно
        """
        super().__init__(e)
        self.seconds = seconds


class DialogMachineModelError(DialogMachineError):
    """ Ошибка модели (модель понимается как часть Модель-Представление-Контроллер) """
    pass


class PhysicalModelError(DialogMachineModelError):
    """ Физическая ошибка модели (нет связи с базой, неправильный синтаксис запроса ...) """
    pass


class LogicalModelError(DialogMachineModelError):
    """ Логическая ошибка в модели (запрос к модели не был выполнен по причинам
    связанным например с другими пользователями), такой запрос должен быть обработан отдельно.

     Пример: один пользователь пытается отправить запрос на покупку какого-то товара, но на момент
     отправки запроса этот товар или его часть уже куплена другим пользователем.
     Соответственно запрос не может быть выполнен, а значит это исключителная ситуация,
     которая отличается от например програмного сбоя, и поэтому должна быть обработана отличным способом"""
    pass


# session_master
class SessionMasterError(DialogMachineError):
    pass

class SessionManagerCollectionError(DialogMachineError):
    """
        Ошибка для DialogMachineError
    """
    pass


class UnknownParentError(SessionMasterError):
    pass


class DeletedParentError(SessionMasterError):
    pass


class FormatDataItemListError(SessionMasterError):
    pass


# base_bulders.py

class BuilderError(DialogMachineError):
    pass
