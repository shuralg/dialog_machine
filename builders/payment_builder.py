# -*- coding: utf-8 -*-

from dialog_machine.builders.base_builders import *
from jinja2 import Environment


class InvoiceVertex(Vertex):
    class ViewMasterInside(ViewMaster):
        def_invoice_dict = dict(
            title='',
            description='',
            provider_token=None,
            currency='RUB',
            photo_url=None,
            photo_height=None,  # !=0/None or picture won't be shown
            photo_width=None,
            photo_size=None,
            is_flexible=False,  # True If you need to set up Shipping Fee
            prices=0,
            start_parameter=None,
            invoice_payload=None,
            need_name=None,
            need_phone_number=None,
            need_email=None,
            need_shipping_address=None,
            disable_notification=None,
            provider_data=None
        )

        def __init__(self, invoice_dict=None, invoice_parameter_name=None):
            """
            :param invoice_dict: параметры платежа
            :param invoice_parameter_name: имя данных для платежа в vert_control_dict, если None,
                    то берем весь vert_control_dict
            """
            super().__init__()
            self.invoice_dict = invoice_dict
            self.invoice_parameter_name = invoice_parameter_name
            self._jinja2_env = Environment()

        def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
            if isinstance(vert_control_dict, dict):
                if self.invoice_parameter_name is None:
                    result = deepcopy(vert_control_dict)
                else:
                    result = vert_control_dict.get(self.invoice_parameter_name, dict())
            else:
                result = dict()
            result['invoice_payload'] = alias_msg
            return result

        def main(self, in_view, dataitem_protect,
                 additional_args: dict, vertex_context_dict: dict):
            invoice_dict = dict()
            for k, v in self.def_invoice_dict.items():
                if k == 'title':
                    invoice_dict[k] = self._jinja2_env.from_string(self.invoice_dict.get(k, v)).render(additional_args)
                elif k == 'description':
                    invoice_dict[k] = self._jinja2_env.from_string(self.invoice_dict.get(k, v)).render(additional_args)
                else:
                    invoice_dict[k] = additional_args.get(k,
                                                          self.invoice_dict.get(k, self.def_invoice_dict.get(k, None)))

            return dict(invoice=invoice_dict)

    class ControllerMasterInside(PrototypeControllerMaster):

        def __init__(self, shipping_command, checkout_command, successful_payment_command,
                     pre_shipping_vertex_name, pre_checkout_vertex_name,
                     pre_successful_payment_vertex_name, invoice_parameter_name):
            super().__init__()
            self.invoice_parameter_name = invoice_parameter_name
            self.shipping_command = shipping_command
            self.checkout_command = checkout_command
            self.successful_payment_command = successful_payment_command
            self.pre_shipping_vertex_name = pre_shipping_vertex_name
            self.pre_checkout_vertex_name = pre_checkout_vertex_name
            self.pre_successful_payment_vertex_name = pre_successful_payment_vertex_name

        def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
            if isinstance(vert_control_dict, dict):
                if self.invoice_parameter_name is None:
                    result = vert_control_dict
                else:
                    result = vert_control_dict.get(self.invoice_parameter_name, dict())
            else:
                result = dict()
            result['invoice_payload'] = alias_msg
            return result

        def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
                 vertex_context_dict: dict):
            msg_control_dict[self.shipping_command] = \
                dict(vertex_name=self.pre_shipping_vertex_name,
                     control_dict=deepcopy(additional_args))
            msg_control_dict[self.checkout_command] = \
                dict(vertex_name=self.pre_checkout_vertex_name,
                     control_dict=deepcopy(additional_args))
            msg_control_dict[self.successful_payment_command] = \
                dict(vertex_name=self.pre_successful_payment_vertex_name,
                     control_dict=deepcopy(additional_args))

            return dataitem_protect, common_control_dict, msg_control_dict

    class VertexControllerMasterInside(PrototypeVertexControllerMaster):

        def __init__(self, vertex_name):
            super().__init__()
            self.vertex_name = vertex_name

        def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
            return vert_control_dict

        def main(self, additional_args: dict, vertex_context_dict: dict):
            """ Метод реализует основной функционал класса
            """
            return self.vertex_name, additional_args

    def create(self, invoice_dict, invoice_parameter_name,
               shipping_command, pre_checkout_command, successful_payment_command,
               pre_shipping_vertex_name, pre_checkout_vertex_name, pre_successful_payment_vertex_name,
               next_vertex_name):
        return self.update_vert(
            vert_blob_list=[
                VertexBlob(
                    view_keeper=self.ViewMasterInside(
                        invoice_dict=invoice_dict,
                        invoice_parameter_name=invoice_parameter_name
                    ),
                    controller_keeper=self.ControllerMasterInside(
                        shipping_command=shipping_command,
                        checkout_command=pre_checkout_command,
                        successful_payment_command=successful_payment_command,
                        pre_shipping_vertex_name=pre_shipping_vertex_name,
                        pre_checkout_vertex_name=pre_checkout_vertex_name,
                        pre_successful_payment_vertex_name=pre_successful_payment_vertex_name,
                        invoice_parameter_name=invoice_parameter_name
                    )
                )
            ],
            controller_vert_keeper=self.VertexControllerMasterInside(next_vertex_name),
            wait_flag_default=next_vertex_name is None
        )


class ShippingVertex(Vertex):
    class ViewMasterInside(ViewMaster):

        def __init__(self, **arg):
            """
            shipping_dict: параметры доставки
                            shipping_query_id=None,
                            ok=True,
                            shipping_options=[],
                            error_message=''
            """
            super().__init__()
            self._arg = arg

        def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
            try:
                shipping_query_id = user_input['shipping_query'].id
            except:
                shipping_query_id = None
            result = dict(
                shipping_query_id=shipping_query_id,
                invouce_data=vert_control_dict
            )
            return result

        def main(self, in_view, dataitem_protect,
                 additional_args: dict, vertex_context_dict: dict):
            ok = self._arg.get('ok', True)
            shipping_options = self._arg.get('shipping_options', list())
            shipping_query_id = additional_args['shipping_query_id']
            error_message = self._arg.get('error_message', '')
            return dict(
                shipping_query_id=shipping_query_id,
                ok=ok,
                shipping_options=shipping_options,
                error_message=error_message
            )

    def create(self, **arg):
        return self.update_vert(
            vert_blob_list=[
                VertexBlob(
                    view_keeper=self.ViewMasterInside(**arg)
                )
            ]
        )


class PreCheckoutVertex(Vertex):
    class ViewMasterInside(ViewMaster):

        def __init__(self, **arg):
            super().__init__()
            self._arg = arg

        def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
            try:
                pre_checkout_query_id = user_input['pre_checkout_query'].id
            except:
                pre_checkout_query_id = None
            result = dict(
                pre_checkout_query_id=pre_checkout_query_id,
                invouce_data=vert_control_dict
            )
            return result

        def main(self, in_view, dataitem_protect,
                 additional_args: dict, vertex_context_dict: dict):
            # pre_checkout_query_id, ok = True,
            # error_message
            ok = self._arg.get('ok', True)
            pre_checkout_query_id = additional_args['pre_checkout_query_id']
            error_message = self._arg.get('error_message', '')
            return dict(
                pre_checkout_query_id=pre_checkout_query_id,
                ok=ok,
                error_message=error_message
            )

    def create(self, **arg):
        return self.update_vert(
            vert_blob_list=[
                VertexBlob(
                    view_keeper=self.ViewMasterInside(**arg)
                )
            ]
        )


class PreHandlerVertex(Vertex):
    param_name = 'data'

    class ControllerMasterInside(PrototypeControllerMaster):
        """ Контроллер для дредобработки данных основного сообщения """

        def __init__(self, param_name):
            super().__init__()
            self.param_name = param_name

        def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
            return vert_control_dict

        def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
                 vertex_context_dict: dict):
            vertex_context_dict[self.param_name] = additional_args

            return dataitem_protect, common_control_dict, msg_control_dict

    class VertexControllerMasterInside(PrototypeVertexControllerMaster):

        def __init__(self, next_vertex_name, param_name):
            super().__init__()
            self.next_vertex_name = next_vertex_name
            self.param_name = param_name

        def main(self, additional_args: dict, vertex_context_dict: dict):
            """ Метод реализует основной функционал класса
            """
            return self.next_vertex_name, vertex_context_dict[self.param_name]

    def create(self, next_vertex_name):
        return self.update_vert(
            vert_blob_list=[
                VertexBlob(
                    controller_keeper=self.ControllerMasterInside(self.param_name)
                )
            ],
            controller_vert_keeper=self.VertexControllerMasterInside(next_vertex_name, self.param_name),
            wait_flag_default=False
        )


class PreSuccessfulPaymentVertex(Vertex):
    class ControllerMasterInside(PrototypeControllerMaster):

        def main(self, dataitem_protect, common_control_dict, msg_control_dict, additional_args: dict,
                 vertex_context_dict: dict):
            common_control_dict.clear()
            msg_control_dict.clear()
            return dataitem_protect, common_control_dict, msg_control_dict

    def create(self, next_vertex_name):
        return self.update_vert(
            vert_blob_list=[
                VertexBlob(
                    controller_keeper=self.ControllerMasterInside()
                )
            ],
            next_vertex_name_def=next_vertex_name,
            wait_flag_default=False
        )


class SuccessfulPaymentVertex(Vertex):
    class ViewMasterInside(ViewMaster):

        def __init__(self, text_template):
            super().__init__()
            self.text_template = text_template
            self._jinja2_env = Environment()

        def switch(self, user_input=None, vert_control_dict=None, env_dict=None, alias_msg=None, **args):
            try:
                result = user_input['message'].successful_payment
            except:
                result = dict()
            return result

        def main(self, in_view, dataitem_protect,
                 additional_args: dict, vertex_context_dict: dict):
            return dict(
                text=self._jinja2_env.from_string(self.text_template).render(additional_args)
            )

    def create(self, next_vertex_name, message_template):
        return self.update_vert(
            vert_blob_list=[
                VertexBlob(
                    view_keeper=self.ViewMasterInside(
                        text_template=message_template
                    )
                )
            ],
            wait_flag_default=False,
            next_vertex_name_def=next_vertex_name
        )


class PaymentBuilder(BaseBuilder):
    shipping_command = '_shipping'
    pre_checkout_command = '_pre_checkout'
    successful_payment_command = '_successful_payment'

    class InvoiceVertexInside(InvoiceVertex):
        pass

    # Для Shipping
    class PreShippingVertexInside(PreHandlerVertex):
        pass

    class ShippingVertexInside(ShippingVertex):
        pass

    # Для PreCheckOut
    class PrePreCheckoutVertexInside(PreHandlerVertex):
        pass

    class PreCheckoutVertexInside(PreCheckoutVertex):
        pass

    # Для successful_payment
    class PreSuccessfulPaymentVertexInside(PreSuccessfulPaymentVertex):
        pass

    class SuccessfulPaymentVertexInside(SuccessfulPaymentVertex):
        pass

    def __init__(self, invoice_vertex_name, shipping_param_dict: dict, pre_checkout_param_dict: dict,
                 shipping_vertex_name=None, checkout_vertex_name=None,
                 successful_payment_vertex_name=None,
                 next_vertex_name=None, next_after_pay_vertex_name=None,
                 invoice_dict=None, invoice_parameter_name=None,
                 successful_payment_msg=None
                 ):
        super().__init__()
        self.invoice_vertex_name = invoice_vertex_name
        self.shipping_vertex_name = shipping_vertex_name if shipping_vertex_name is not None \
            else f'{self.invoice_vertex_name}_shipping'
        self.checkout_vertex_name = checkout_vertex_name if checkout_vertex_name is not None \
            else f'{self.invoice_vertex_name}_checkout'
        self.successful_payment_vertex_name = successful_payment_vertex_name \
            if successful_payment_vertex_name is not None \
            else f'{self.invoice_vertex_name}_successful_payment'

        self.pre_shipping_vertex_name = f'pre_{self.invoice_vertex_name}_shipping'
        self.pre_checkout_vertex_name = f'pre_{self.invoice_vertex_name}_checkout'
        self.pre_successful_payment_vertex_name = f'pre_{self.invoice_vertex_name}_successful_payment'

        self.next_vertex_name = next_vertex_name
        self.next_after_pay_vertex_name = next_after_pay_vertex_name
        self.invoice_parameter_name = invoice_parameter_name
        self.invoice_dict = invoice_dict if isinstance(invoice_dict, dict) else dict()

        self.shipping_param_dict = shipping_param_dict
        self.pre_checkout_param_dict = pre_checkout_param_dict

        self.successful_payment_msg = successful_payment_msg

    def build_a(self, vertex_dict):
        try:
            vertex_dict[self.invoice_vertex_name]
        except KeyError:
            pass
        else:
            vertex_dict[self.invoice_vertex_name] = self.InvoiceVertexInside().create(
                invoice_dict=self.invoice_dict,
                invoice_parameter_name=self.invoice_parameter_name,
                shipping_command=self.shipping_command,
                pre_checkout_command=self.pre_checkout_command,
                successful_payment_command=self.successful_payment_command,
                pre_checkout_vertex_name=self.pre_checkout_vertex_name,
                pre_shipping_vertex_name=self.pre_shipping_vertex_name,
                pre_successful_payment_vertex_name=self.pre_successful_payment_vertex_name,
                next_vertex_name=self.next_vertex_name
            )
            vertex_dict[self.pre_shipping_vertex_name] = self.PreShippingVertexInside().create(
                next_vertex_name=self.shipping_vertex_name
            )
            vertex_dict[self.shipping_vertex_name] = self.ShippingVertexInside().create(
                **self.shipping_param_dict
            )
            vertex_dict[self.pre_checkout_vertex_name] = self.PrePreCheckoutVertexInside().create(
                next_vertex_name=self.shipping_vertex_name
            )
            vertex_dict[self.checkout_vertex_name] = self.PreCheckoutVertexInside().create(
                **self.pre_checkout_param_dict
            )
            if self.successful_payment_msg is not None:
                vertex_dict[self.pre_successful_payment_vertex_name] = self.PreSuccessfulPaymentVertexInside().create(
                    next_vertex_name=self.successful_payment_vertex_name
                )
                vertex_dict[self.successful_payment_vertex_name] = self.SuccessfulPaymentVertexInside().create(
                    next_vertex_name=self.next_vertex_name,
                    message_template=self.successful_payment_msg
                )
            else:
                vertex_dict[self.pre_successful_payment_vertex_name] = self.PreSuccessfulPaymentVertexInside().create(
                    next_vertex_name=self.next_vertex_name
                )
        return vertex_dict
