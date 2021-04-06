'''Monolith for testing and prototyping

'''
import sys

from dataclasses import dataclass

@dataclass
class Company:
    name : str
    ticker : str
    stock_exchange : str
    description : str

