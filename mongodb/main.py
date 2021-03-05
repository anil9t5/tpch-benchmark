# -*- coding: utf-8 -*-
import sys
from mongodb.collections import Collections


def main():
    print("mongodb module")

    task = Collections()
    task.create_collections()
