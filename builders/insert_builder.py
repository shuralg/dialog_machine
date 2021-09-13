# -*- coding: utf-8 -*-

from dialog_machine.builders.base_builders import *
from dialog_machine.dialog_machine_core import *
from dialog_machine.session_master import DataItemConnectorBuilder
from dialog_machine.project_exceptions import DialogMachineError

from dialog_machine.builders.dialog_item_builder import SimpleVertexControllerMaster, DialogItemBuilder

from dialog_machine.dialog_machine_core import ModelMasterAbstract

from dialog_machine.view_masters import ViewSwitch, ComplexViewMaster


class PrivateControllerVertexError(DialogMachineError):
    pass


class AbstractControllerVertexDialogItem(ComplexVertexControllerMaster):
    """ Абстрактный класс для контроллера вершины DialogItem """
    _key_name = "value"
    _except_msg = dict()
    _except_key = "except_msg"
    _control_dict_param_name = "num_list_of_response"
    # параметры для перехода (имя вершины и управляющие данные) в случае программной ошибки
    _error_vertex_name = None
    _error_control_dict = dict()
    _list_of_responds = list()
    _user_input_def = ['text']

    def __init__(self, list_of_response=None, except_msg=None, control_dict_param_name=None,
                 key_name=None, error_dict=None, user_input_key=None, **args):
        """
        :param list_of_response: предустановленный список значений параметра
        :param except_msg: сообщение, если ввод оказался неуспешным
        :param control_dict_param_name: имя параметра в control_dict
        :param key_name: имя параметра результата, в случае успешной проверки
        :param error_dict: словарь параметров перехода (имя вершины и control_dict) в случае программной ошибки
                            (несоответствие формата ввода)
        :param user_input_key: list - список используемых полей пользовательского ввода (['text'])
        """
        super().__init__(user_input_key=user_input_key if user_input_key is not None else self._user_input_def)

        args['list_of_response'] = list_of_response
        args['except_msg'] = except_msg
        args['control_dict_param_name'] = control_dict_param_name
        args['key_name'] = key_name
        args['error_dict'] = error_dict

        self.set_param(**args)

    def set_param(self, list_of_response=None, except_msg=None, control_dict_param_name=None,
                  key_name=None, error_dict=None, **args):
        super().set_param(**args)

        if list_of_response is not None:
            assert isinstance(list_of_response, list)
            for v in list_of_response:
                assert isinstance(v, dict)
                assert "value" in v.keys()  # Там больжен быть еще параметр text, но нас интересует только value
            self._list_of_response = list_of_response

        if except_msg is not None:
            self._except_msg = except_msg

        if type(control_dict_param_name) is str:
            self._control_dict_param_name = control_dict_param_name

        if type(key_name) is str:
            self._key_name = key_name

        if isinstance(error_dict, dict):
            try:
                self._error_vertex_name = error_dict['vertex_name']
                self._error_control_dict = error_dict['control_dict']
            except KeyError:
                pass

    @abstractmethod
    def _parser(self, arg):
        """ Возвращает значение в нужном типе и формате
            в случае невозможности парсинга генерируем исключение PrivateControllerVertexError
        """
        return None

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        result = dict()

        result['control_dict'] = deepcopy(vert_control_dict) if vert_control_dict is not None else None

        if user_input is not None:
            res_u_i = None
            for k in self._user_input_key:
                try:
                    res_u_i = user_input[k]
                except KeyError:
                    pass
                else:
                    break
            result['user_input'] = res_u_i
        else:
            result['user_input'] = None

        return result

    def main(self, additional_args: dict, vertex_context_dict: dict):
        try:
            temp = self._vertex_list[0]
        except IndexError:
            next_vertex_name, next_control_dict = None, dict()
        else:
            try:
                next_vertex_name, next_control_dict = temp['vertex_name'], temp['control_dict']
            except KeyError:
                next_vertex_name, next_control_dict = None, dict()

        try:
            temp = self._vertex_list[1]
        except IndexError:
            except_vertex_name, except_control_dict = next_vertex_name, next_control_dict
        else:
            try:
                except_vertex_name, except_control_dict = temp['vertex_name'], temp['control_dict']
            except KeyError:
                except_vertex_name, except_control_dict = next_vertex_name, next_control_dict

        t_control_dict = additional_args.get('control_dict', None)
        if isinstance(t_control_dict, dict) and t_control_dict.get(self._control_dict_param_name, None) is not None:
            index_list = t_control_dict[self._control_dict_param_name]
            try:
                temp_d = self._list_of_response[index_list]
            except IndexError:
                return self._error_vertex_name, self._error_control_dict
            if not isinstance(temp_d, dict):
                return self._error_vertex_name, self._error_control_dict
            try:
                value = temp_d['value']
            except KeyError:
                return self._error_vertex_name, self._error_control_dict
            next_control_dict[self._key_name] = value
            return next_vertex_name, next_control_dict
        else:
            # Берем данные из пользовательского ввода, но их надо будет уже проверять на соответствие формату
            user_input = additional_args.get('user_input', None)
            if user_input is not None:
                try:
                    value = self._parser(user_input)
                except PrivateControllerVertexError:
                    pass
                else:
                    next_control_dict[self._key_name] = value
                    return next_vertex_name, next_control_dict
            except_control_dict[self._except_key] = self._except_msg
            return except_vertex_name, except_control_dict


class StringControllerVertexDialogItem(AbstractControllerVertexDialogItem):
    """ Ввод строки """

    def _parser(self, arg):

        if arg is None:
            raise PrivateControllerVertexError("")

        try:
            return str(arg)
        except Exception:
            raise PrivateControllerVertexError("")


class NaturalNumberControllerVertexDialogItem(AbstractControllerVertexDialogItem):
    """ Ввод натурального числа """

    def _parser(self, arg):
        if arg is None:
            raise PrivateControllerVertexError("")

        try:
            return abs(int(arg))
        except Exception:
            raise PrivateControllerVertexError("")


class ControllerDialogItem(ComplexControllerMaster):
    _dict_name = "keyboard_dict"
    _callback_name = "callback_data"

    def __init__(self, dict_name=None, callback_name=None, **args):
        super().__init__()
        args['dict_name'] = dict_name
        args['callback_name'] = callback_name
        self.set_param(**args)

    def set_param(self, dict_name=None, callback_name=None, **args):
        """
            dict_name - имя параметра, в котором хранится список для клавиатуры
        """
        if dict_name is not None:
            assert type(dict_name) is str
            self._dict_name = dict_name
        if callback_name is not None:
            assert type(callback_name) is str
            self._callback_name = callback_name
        super().set_param(**args)

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        """
        Основной метод выполнения объекта данного класса
        Обеспечивает реализацию основной логики контроллера.
        Возвращает common_control_dict (без привязки к конкретному сообщению)
            и msg_control_dict (словарь команд с привязкой к сообщению) для соответствующего DataItem
        В процессе выполнения модифицирует входящий data_item_protect
        :param vertex_context_dict:
        """
        name_def = 'cont_param_{}'
        result = dict()
        # logging.debug("___Controller Master: {}".format(self._vertex_list))
        for id_, v in enumerate(self._vertex_list):
            field_name = name_def.format(id_)

            # result[id_] = dict(callback_data=field_name, text=v.get('field_caption', '-'))
            result[id_] = {self._callback_name: field_name, "text": v.get('field_caption', '-')}
            # temp_cd = dict(vertex_name=v['vertex_name'], control_dict=v['control_dict'])
            msg_control_dict[field_name] = dict(vertex_name=v['vertex_name'], control_dict=v['control_dict'])

        dataitem_protect[self._dict_name] = result
        return dataitem_protect, common_control_dict, msg_control_dict


class ViewSwitchVertexBlobDialogItem(ViewSwitch):
    """ Класс коммутатора для View.
        Объект принимает на вход входное представление (input view) и генерируемое представление (generated view)
        отрабатывает логику компоновки этих представлений и выдает результирующее View
    """
    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict()

    def main(self, in_view, gen_view, additional_args: dict, vertex_context_dict: dict):
        for k, v in gen_view.items():
            try:
                in_v = in_view[k]
            except KeyError:
                in_view[k] = v
            else:
                if type(v) is str and type(in_v) is str:
                    in_view[k] = in_v + v
                elif type(v) is list and type(in_v) is list:
                    in_v.extend(v)
                    in_view[k] = in_v
        return in_view


class InsertItemVertex(BaseBuilder):
    """  Создание отдельного элемента для ввода """
    _name_of_choice_param = 'choice_param'

    _suffix = '_response'
    _upd_suf = '_updater'

    def __init__(self, vertex_name: str, next_vertex_name: str, exception_vertex_name: str,
                 data_item_name: str, parent_data_item_name,
                 vertex_blob_request: VertexBlobBuilder,
                 vertex_blob_response: VertexBlobBuilder,
                 param_name: str, controller_vertex: AbstractControllerVertexDialogItem,
                 list_of_response: list,
                 handlers_dict: dict,
                 type_handler: str = 'inline_keyboard'):
        """
        :param vertex_name: имя вершины
        :param next_vertex_name: имя следующей вершины
        :param exception_vertex_name: имя вершины в случае нештатных действий пользователя
        :param data_item_name: имя экземпляра локальной сессии
        :param parent_data_item_name: имя родителя для экземпляра локальной сессии
        :param vertex_blob_request: VertexBlobBuilder,
        :param vertex_blob_response: VertexBlobBuilder,
        :param param_name: имя вводимого параметра
        :param controller_vertex: котроллер вершины
        :param list_of_response: список параметров для выбора
                                        формат: [dict(text=..., value=...), ...]
        :param handlers_dict: список обработчиков для создания ViewMaster
        :param type_handler: тип обработчика в представлении, который будет обеспечивать
                            выбор дополнительных параметров
        """
        assert type(vertex_name) is str
        assert type(next_vertex_name) is str
        assert type(exception_vertex_name) is str
        assert type(data_item_name) is str
        assert type(param_name) is str
        assert type(parent_data_item_name) is str or parent_data_item_name is None
        assert isinstance(vertex_blob_request, VertexBlobBuilder)
        assert isinstance(vertex_blob_response, VertexBlobBuilder)
        assert isinstance(controller_vertex, AbstractControllerVertexDialogItem)
        assert isinstance(list_of_response, list)
        assert isinstance(handlers_dict, dict)
        assert type(type_handler) is str

        for v in list_of_response:
            assert isinstance(v, dict)
            assert set(v.keys()) == {'text', 'value'}
            assert type(v['text']) is str

        super().__init__()
        self._vertex_name = vertex_name
        self._next_vertex_name = next_vertex_name
        self._exception_vertex_name = exception_vertex_name
        self._data_item_name = data_item_name
        self._parent_data_item_name = parent_data_item_name
        self._vertex_blob_request = vertex_blob_request
        self._vertex_blob_response = vertex_blob_response
        self._param_name = param_name
        self._controller_vertex = controller_vertex
        self._list_of_response = list_of_response
        self._handlers_dict = handlers_dict
        self._type_handler = type_handler

    def build_a(self, vertex_dict):
        vertex_upd_name = "{}{}".format(self._vertex_name, self._upd_suf)
        # vertex_upd_name - вершина должна удалять inline клавиатуру из сообщения
        vertex_name_response = "{}{}".format(self._vertex_name, self._suffix)

        # Установка параметров в созданный контроллер вершины
        self._controller_vertex.set_param(list_of_response=self._list_of_response,
                                          key_name=self._param_name,
                                          control_dict_param_name=self._name_of_choice_param)

        # def __init__(self, vertex_name: str, vertex_blob_request: VertexBlobBuilder,
        #              next_vertex_name: str, except_vertex_name: str,
        #              vertex_controller: AbstractVertexControllerMaster,
        #              vertex_blob_response: VertexBlobBuilder,
        #              data_item_name: str, vertex_name_response: str, vertex_upd_name: str,
        #              parent_data_item_name=None)
        builder = DialogItemBuilder(vertex_name=self._vertex_name,
                                    vertex_blob_request=self._vertex_blob_request,
                                    next_vertex_name=self._next_vertex_name,
                                    except_vertex_name=self._exception_vertex_name,
                                    vertex_controller=self._controller_vertex,
                                    vertex_blob_response=self._vertex_blob_response,
                                    data_item_name=self._data_item_name,
                                    vertex_name_response=vertex_name_response,
                                    vertex_upd_name=vertex_upd_name,
                                    parent_data_item_name=self._parent_data_item_name)
        vertex_dict = builder.build(vertex_dict)

        # Необходимо добавить VertexBlob с клавиатурой
        template = {self._type_handler: [[dict(id=i, text=v['text'])] for i, v in enumerate(self._list_of_response)]}
        view_keeper = ComplexViewMaster(template=template, view_switch=ViewSwitchVertexBlobDialogItem(),
                                        handlers_dict=self._handlers_dict)
        dict_param = view_keeper.get_param(self._type_handler)
        vertex_blob_keyboard = VertexBlob(local_session_builder=DataItemConnectorBuilder(None, None),
                                          model_keeper=None,
                                          view_keeper=view_keeper,
                                          controller_keeper=ControllerDialogItem(
                                              vertex_list=[dict(vertex_name=vertex_upd_name,
                                                                control_dict={self._name_of_choice_param: i})
                                                           for i, v in enumerate(self._list_of_response)],
                                              **dict_param)
                                          )
        # добавляем этот vertex_blob в вершину
        try:
            vertex_main = vertex_dict[self._vertex_name]
        except KeyError:
            pass
        else:
            vertex_main.update_vert(vert_blob_list=[vertex_blob_keyboard])
            vertex_dict[self._vertex_name] = vertex_main
        return vertex_dict


class InsertBuilder(BaseBuilder):
    _insert_suf = 'insert'

    def __init__(self, vertex_name: str, insert_model: ModelMasterAbstract, ins_items_list: list, next_vertex_name: str,
                 except_vertex_name: str,
                 data_item_name, data_item_parent, handlers_dict, view_template_insert=None,
                 type_handler='inline_keyboard'):
        """

        :type view_template_insert: dict
                                    шаблон ответа при вставке
        :type handlers_dict: dict
                                список обработчиков шаблона
        :type except_vertex_name: str
                                    имя вершины, в которую необходимо перейти,
                                    если при выполнении модели какие-то проблемы
        :type data_item_name: str or None
                                    имя элемента сессии
        :type data_item_parent: str or None
                                    имя родителя элемента сессии
        :type vertex_name: str
                                    имя начальной вершины
        :type insert_model: ModelMasterAbstract
                                    модель для вставки
        :type ins_items_list: list
                                    список вставки [dict(item_name, text, text_response, text_except,
                                                list_of_responds,
                                                vertex_blob_request, vertex_blob_responds, vertex_blob_except,
                                                controller_vertex),...]
                    list_of_responds: [dict(text, value)]
        :type next_vertex_name: str or None
                                    имя вершины, в которую необходимо перейти
        """
        super().__init__()
        self._vertex_name = vertex_name
        self._insert_model = insert_model
        self._ins_items_list = ins_items_list
        self._next_vertex_name = next_vertex_name
        self._data_item_name = data_item_name
        self._data_item_parent = data_item_parent
        self._handlers_dict = handlers_dict
        self._type_handler = type_handler
        self._view_template_insert = view_template_insert if view_template_insert is not None else dict()
        self._except_vertex_name = except_vertex_name

    def append_ins_item_list(self, item_name, text, controller_vertex,
                             text_response=None, text_except=None,
                             list_of_responds=None, vertex_blob_request=None, vertex_blob_responds=None,
                             vertex_blob_except=None):
        list_of_responds = list_of_responds if type(list_of_responds) is list else list()
        item_list = dict(item_name=item_name, text=text, controller_vertex=controller_vertex,
                         text_response=text_response, text_except=text_except,
                         list_of_responds=list_of_responds)
        if vertex_blob_request is not None:
            item_list['vertex_blob_request'] = vertex_blob_request
        if vertex_blob_responds is not None:
            item_list['vertex_blob_responds'] = vertex_blob_responds
        if vertex_blob_except is not None:
            item_list['vertex_blob_except'] = vertex_blob_except

        self._ins_items_list.append(item_list)

    def build_a(self, vertex_dict):
        cur_vertex_name = self._vertex_name

        if self._insert_model is not None:
            insert_vertex_name = "{}_{}".format(self._vertex_name, self._insert_suf)
            # Добавляем вершину ввода данных в модель
            vertex_blob_insert = VertexBlobBuilder(controller_keeper=ComplexControllerMaster(),
                                                   view_keeper=ComplexViewMaster(
                                                       template=self._view_template_insert,
                                                       handlers_dict=self._handlers_dict),
                                                   data_item=self._data_item_name,
                                                   parent_data_item=self._data_item_name,
                                                   model_keeper=self._insert_model)
            vertex_insert = VertexBuilder(vert_blob_list=[vertex_blob_insert.build()],
                                          wait_signal_bool=False,
                                          controller_vert_keeper=SimpleVertexControllerMaster(
                                              [dict(vertex_name=self._next_vertex_name,
                                                    control_dict=dict())]),
                                          next_vertex_name_def=self._except_vertex_name
                                          )
            vertex_dict[insert_vertex_name] = vertex_insert.build()
        else:
            insert_vertex_name = self._next_vertex_name

        next_vertex_name_list = ["{}_{}".format(self._vertex_name, v['item_name'])
                                 for i, v in enumerate(self._ins_items_list) if i != 0]
        next_vertex_name_list.append(insert_vertex_name)

        data_item_parent = self._data_item_parent

        for v, next_vertex_name in zip(self._ins_items_list, next_vertex_name_list):
            # Создаем вершину-исключение
            exception_vertex_name = "{}_{}".format('except', v['item_name'])
            try:
                vertex_blob_except = v['vertex_blob_except']
            except KeyError:
                try:
                    temp_except = v['text_except']
                except KeyError:
                    vertex_blob_except = None
                    exception_vertex_name = cur_vertex_name
                else:
                    vertex_blob_except = VertexBlobBuilder(controller_keeper=ComplexControllerMaster(),
                                                           view_keeper=ComplexViewMaster(
                                                               template=dict(text=temp_except),
                                                               handlers_dict=self._handlers_dict))
            if vertex_blob_except is not None:
                vertex_controller_except = SimpleVertexControllerMaster(
                    [dict(vertex_name=cur_vertex_name, control_dict=dict())])
                vertex_blob_except.data_item(self._data_item_name)
                vertex_blob_except.parent_data_item(self._data_item_name)
                vertex_except_builder = VertexBuilder(vert_blob_list=[vertex_blob_except.build()],
                                                      dict_id_getter=None,
                                                      wait_signal_bool=False,
                                                      controller_vert_keeper=vertex_controller_except)
                vertex_dict[exception_vertex_name] = vertex_except_builder.build()
            try:
                vertex_blob_request = v['vertex_blob_request']
            except KeyError:
                vertex_blob_request = VertexBlobBuilder(controller_keeper=ComplexControllerMaster(),
                                                        view_keeper=ComplexViewMaster(
                                                            template=dict(text=v['text']),
                                                            handlers_dict=self._handlers_dict))

            try:
                vertex_blob_response = v['vertex_blob_responds']
            except KeyError:
                vertex_blob_response = VertexBlobBuilder(controller_keeper=ComplexControllerMaster(),
                                                         view_keeper=ComplexViewMaster(
                                                             template=dict(text=v['text_response']),
                                                             handlers_dict=self._handlers_dict))

            insert_item_vertex = InsertItemVertex(vertex_name=cur_vertex_name,
                                                  next_vertex_name=next_vertex_name,
                                                  exception_vertex_name=exception_vertex_name,
                                                  data_item_name=self._data_item_name,
                                                  parent_data_item_name=data_item_parent,
                                                  vertex_blob_request=vertex_blob_request,
                                                  vertex_blob_response=vertex_blob_response,
                                                  param_name=v['item_name'],
                                                  controller_vertex=v['controller_vertex'],
                                                  list_of_response=v['list_of_responds'],
                                                  handlers_dict=self._handlers_dict,
                                                  type_handler=self._type_handler)
            data_item_parent = self._data_item_name
            cur_vertex_name = next_vertex_name
            vertex_dict = insert_item_vertex.build(vertex_dict)

        insert_vertex_name = "{}_{}".format(self._vertex_name, self._insert_suf)
        # TODO: добавить вершину с выполнением запроса INSERT
        return vertex_dict
