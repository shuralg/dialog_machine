# -*- coding: utf-8 -*-

from dialog_machine.builders.base_builders import *
from dialog_machine.dialog_machine_core import *
from dialog_machine.project_exceptions import DialogMachineError


class PrivateControllerVertexError(DialogMachineError):
    pass


class ItemInputController(PrototypeControllerMaster):
    """ Контроллер элемента ввода (для ввода из inline клавиатуры) """

    def __init__(self, update_callback_vertex_name: str,
                 vertex_dict: dict, param_name: str, dict_param_name=None):
        """

        :param update_callback_vertex_name: имя вершины графа состояний,
                        которая будет обрабатывать callback-и
        :param vertex_dict: {<id1>:<значение1>,..}
        :param param_name: - имя вводимого параметра
        :param dict_param_name: - имя словаря параметров
        """
        super().__init__()
        self._update_callback_vertex_name = update_callback_vertex_name
        self._vertex_dict = vertex_dict
        self._param_name = param_name
        self._dict_param_name = dict_param_name

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return vert_control_dict if vert_control_dict is not None else dict()

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        for k, v in self._vertex_dict.items():
            control_dict = deepcopy(additional_args)
            if self._dict_param_name is None:
                control_dict[self._param_name] = v
            else:
                temp = control_dict.get(self._dict_param_name, dict())
                temp[self._param_name] = v
                control_dict[self._dict_param_name] = temp
            msg_control_dict[k] = dict(vertex_name=self._update_callback_vertex_name,
                                       control_dict=control_dict)
        return dataitem_protect, common_control_dict, msg_control_dict


class ItemInputView(ViewMaster):
    """ Объект представления для элемента ввода данных """

    def __init__(self, text: str, inline_keyboard_list=None, keyboard_list=None):
        """
        :param text: - текст сообщения
        :param inline_keyboard_list:  - данные для вывода inline клавиатуры
                    [[dict(id=..., text=..., url=...)...],
                     [dict(id=..., text=..., url=...)...],dict(id=..., text=..., url=...),
                     ...
                     ]
        :param keyboard_list: - данные для ввода обычной клавиатуры
                     [[text1...],
                     [text2...],text3...),
                     ...
                     ]
        """
        super().__init__()
        self._text = text
        self._inline_keyboard_list = inline_keyboard_list
        self._keyboard_list = keyboard_list

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        in_view['text'] = self._text
        if self._inline_keyboard_list is not None:
            inline_kb = list()
            for v in self._inline_keyboard_list:
                if type(v) is list:
                    line_kb = list()
                    for vv in v:
                        try:
                            url = vv['url']
                        except KeyError:
                            line_kb.append(dict(callback_data=vv['id'], text=vv['text']))
                        else:
                            line_kb.append(dict(url=url, text=vv['text']))
                    if len(line_kb) > 0:
                        inline_kb.append(line_kb)
                else:
                    try:
                        url = v['url']
                    except KeyError:
                        inline_kb.append(dict(callback_data=v['id'], text=v['text']))
                    else:
                        inline_kb.append(dict(url=url, text=v['text']))
            if len(inline_kb) > 0:
                in_view['inline_keyboard'] = inline_kb
        elif self._keyboard_list is not None:
            kb = list()
            for v in self._keyboard_list:
                if type(v) is list:
                    line_list = [dict(text=vv) for vv in v]
                    if len(line_list) > 0:
                        kb.append(line_list)
                elif type(v) is str:
                    kb.append(v)
            if len(kb) > 0:
                in_view['keyboard'] = kb

        return in_view


class UniversalItemInputVertexController(PrototypeVertexControllerMaster):
    """ Контроллер вершины элемента ввода данных """

    def __init__(self, next_vertex_name: str):
        """
        :param next_vertex_name: - имя вершины для штатного перехода
        """
        super().__init__()
        self._next_vertex_name = next_vertex_name

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return deepcopy(vert_control_dict) if vert_control_dict is not None else dict()

    def main(self, additional_args: dict, vertex_context_dict: dict):
        """ Метод реализует основной функционал класса
        :param vertex_context_dict:
        """
        return self._next_vertex_name, additional_args


class UpdateItemInputView(ViewMaster):
    """ View для обработчика callback """

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        try:
            del in_view['inline_keyboard']
        except KeyError:
            pass
        try:
            del in_view['keyboard']
        except KeyError:
            pass

        return in_view


class UpdateItemInputController(PrototypeControllerMaster):

    def __init__(self):
        super().__init__()

    def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
             vertex_context_dict: dict):
        common_control_dict.clear()
        msg_control_dict.clear()
        return dataitem_protect, common_control_dict, msg_control_dict


class AbstractUpdateItemInputVertexController(PrototypeVertexControllerMaster):
    """ Контроллер вершины элемента ввода данных """
    _def_callback_list = list()

    def __init__(self, except_vertex_name=None,
                 next_vertex_name=None, param_name=None, dict_param_name=None):
        """
        :param except_vertex_name: - имя вершины, в которую нужно перейти при неправильном вводе
        :param next_vertex_name: - имя вершины для штатного перехода
        :param param_name: - имя параметра, который вводится
        :param dict_param_name: - имя словаря параметров
        """
        super().__init__()
        self._except_vertex_name = except_vertex_name
        self._next_vertex_name = next_vertex_name
        self._param_name = param_name
        self._dict_param_name = dict_param_name

    def set_param(self, except_vertex_name: str,
                  next_vertex_name: str, param_name: str, dict_param_name=None):
        """
        :param except_vertex_name: - имя вершины, в которую нужно перейти при неправильном вводе
        :param next_vertex_name: - имя вершины для штатного перехода
        :param param_name: - имя параметра, который вводится
        :param dict_param_name: - имя словаря параметров для ввода
        """
        if except_vertex_name is not None:
            self._except_vertex_name = except_vertex_name
        if next_vertex_name is not None:
            self._next_vertex_name = next_vertex_name
        if param_name is not None:
            self._param_name = param_name
        if dict_param_name is not None:
            self._dict_param_name = dict_param_name

    @abstractmethod
    def _parser(self, input_msg, param_name):
        """ Абстрактный метод, преобразующий пользовательский ввод к нужному формату
            или вызывающий исключение PrivateControllerVertexError в случае,
            если это невозможно сделать"""
        value = dict()
        return value

    def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
        return dict(data=vert_control_dict, input=user_input.message)

    def main(self, additional_args: dict, vertex_context_dict: dict):
        """ Метод реализует основной функционал класса
        :param vertex_context_dict:
        """
        data = additional_args.get('data', dict())
        data = data if data is not None else dict()
        try:
            value_dict = self._parser(additional_args['input'], self._param_name)
        except PrivateControllerVertexError:
            return self._except_vertex_name, data
        except KeyError:
            return self._except_vertex_name, data
        else:
            if self._dict_param_name is None:
                for k, v in value_dict.items():
                    data[k] = v
            else:
                try:
                    temp = data[self._dict_param_name]
                except KeyError:
                    temp = dict()
                except TypeError:
                    temp = dict()
                for k, v in value_dict.items():
                    temp[k] = v
                data[self._dict_param_name] = temp

        return self._next_vertex_name, data


class ExceptItemInputView(ViewMaster):
    """ View для исключения (если ввод был неправильным) """

    def __init__(self, text: str):
        super().__init__()
        self._text = text

    def main(self, in_view, dataitem_protect, additional_args: dict, vertex_context_dict: dict):
        in_view['text'] = self._text
        return in_view


class InsertItemsBuilderNew(BaseBuilder):
    """ Билдер комплексной вставки параметров """
    _model_vertex_suf = 'model_vertex'
    _update_suf = 'handler_item'
    _except_suf = 'except_item'
    _callback_suf = 'callback_item'
    _list_def = list()
    _dict_def = dict()

    def __init__(self, begin_vertex_name: str,
                 next_vertex_name: str, dict_param_name=None):
        super().__init__()
        self._begin_vertex_name = begin_vertex_name
        self._next_vertex_name = next_vertex_name
        self._input_items_list = list()
        self._model_vertex_name = next_vertex_name
        self._dict_param_name = dict_param_name

    def input_item_append(self, param_name: str, text: str, except_text,
                          parser_controller: AbstractUpdateItemInputVertexController,
                          inline_keyboard_list=None, keyboard_list=None):
        """
        :param param_name: имя вводимого параметра
        :param text: текст сообщения
        :param except_text: текст сообщения исключения
        :param parser_controller: контроллер, который парсит
        :param inline_keyboard_list:  - данные для вывода inline клавиатуры
                    [[dict(id=..., text=..., url=..., value=...)...],
                     [dict(id=..., text=..., url=..., value=...)...],
                     dict(id=..., text=..., url=..., value=...),
                     ...
                     ]
        :param keyboard_list: - данные для ввода обычной клавиатуры
                     [[text1...],
                     [text2...],text3...),
                     ...
                     ]
        :return:
        """
        assert isinstance(parser_controller, AbstractUpdateItemInputVertexController)

        if keyboard_list is None:
            keyboard_list = self._list_def
        if inline_keyboard_list is None:
            inline_keyboard_list = self._list_def

        self._input_items_list.append(dict(param_name=param_name,
                                           text=text,
                                           except_text=except_text,
                                           parser_controller=parser_controller,
                                           inline_keyboard_list=inline_keyboard_list,
                                           keyboard_list=keyboard_list))

    def _handle_input_items_list(self):
        """ Обрабатываем input_items_list """
        result = list()

        names_list = ['{}_{}'.format(self._begin_vertex_name, v['param_name'])
                      for v in self._input_items_list]
        names_list.append(self._next_vertex_name)

        flag_first = True
        for v, next_vertex_name in zip(self._input_items_list, names_list[1:]):
            if flag_first:
                item_vertex_name = self._begin_vertex_name
                flag_first = False
            else:
                item_vertex_name = '{}_{}'.format(self._begin_vertex_name, v['param_name'])
            upd_item_vertex_name = '{}_{}'.format(item_vertex_name, self._update_suf)
            callback_item_vertex_name = '{}_{}'.format(item_vertex_name, self._callback_suf)
            except_item_vertex_name = '{}_{}'.format(item_vertex_name, self._except_suf)

            item_vertex = Vertex(
                vert_blob_list=[VertexBlob(
                    view_keeper=ItemInputView(
                        text=v['text'],
                        inline_keyboard_list=v['inline_keyboard_list'],
                        keyboard_list=v['keyboard_list']),
                    controller_keeper=ItemInputController(
                        update_callback_vertex_name=callback_item_vertex_name,
                        vertex_dict={vv['id']: vv['value'] for vv in v['inline_keyboard_list']},
                        param_name=v['param_name'],
                        dict_param_name=self._dict_param_name)
                ), ],
                controller_vert_keeper=UniversalItemInputVertexController(
                    next_vertex_name=upd_item_vertex_name
                ),
                wait_flag_default=True
            )

            callback_item_vertex = Vertex(
                vert_blob_list=[VertexBlob(
                    view_keeper=UpdateItemInputView(),
                    controller_keeper=UpdateItemInputController()
                ), ],
                controller_vert_keeper=UniversalItemInputVertexController(
                    next_vertex_name=next_vertex_name
                ),
                wait_flag_default=False
            )

            parser_controller = v['parser_controller']
            parser_controller.set_param(
                except_vertex_name=except_item_vertex_name,
                next_vertex_name=next_vertex_name,
                param_name=v['param_name'],
                dict_param_name=self._dict_param_name)

            upd_item_vertex = Vertex(
                vert_blob_list=[VertexBlob(
                    view_keeper=UpdateItemInputView(),
                    controller_keeper=UpdateItemInputController()
                ), ],
                controller_vert_keeper=parser_controller,
                wait_flag_default=False
            )

            except_item_vertex = Vertex(
                vert_blob_list=[VertexBlob(
                    view_keeper=ExceptItemInputView(
                        text=v['except_text']
                    ),
                    controller_keeper=UpdateItemInputController()
                ), ],
                controller_vert_keeper=UniversalItemInputVertexController(
                    next_vertex_name=item_vertex_name
                ),
                wait_flag_default=False
            )

            result.append((item_vertex_name, item_vertex))
            result.append((callback_item_vertex_name, callback_item_vertex))
            result.append((upd_item_vertex_name, upd_item_vertex))
            result.append((except_item_vertex_name, except_item_vertex))

        return result

    def build_a(self, vertex_dict):
        for k, v in self._handle_input_items_list():
            vertex_dict[k] = v
        return vertex_dict
