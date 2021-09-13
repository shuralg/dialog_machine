#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

from dialog_machine.builders.base_builders import *
from dialog_machine.dialog_machine_core import *

""" Билдер для взаимодействия с таблицей.
    Реализуем базовый функционал выбора (select), вставки (insert), обновления (update) и удаления (delete)
"""

# Билдер

class WorkTableBuilder(BaseBuilder):

    def __init__(self, vertex_name: str, select_model: ModelMasterAbstract):
        super().__init__()
        pass