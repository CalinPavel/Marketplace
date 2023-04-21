"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""


import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('marketplace.log', maxBytes=4000, backupCount=10)
logger.addHandler(handler)

from threading import Lock, currentThread

class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """

        self.queue_size_per_producer = queue_size_per_producer

        self.return_producer_id = Lock() #register producer
        self.producer_queue = []
        self.products = []
        self.producers_dic = {}

        self.create_new_cart = Lock() #register producer
        self.modify_cart = Lock() #register producer

        self.cart_count=0
        self.cart_dic = {}

        self.print_mutex = Lock() #register producer


    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """

        with self.return_producer_id:
            self.producer_queue.append(0)
            logger.info('register_producer')
            return len(self.producer_queue)-1

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """

        id = int(producer_id)

        if self.producer_queue[id] >= self.queue_size_per_producer:
            return False
        
        self.producer_queue[id] += 1
        self.products.append(product)
        self.producers_dic[product] = id
        logger.info('publish')

        return True

        pass

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """

        with self.create_new_cart:
            self.cart_count += 1
            cart_id = self.cart_count

        self.cart_dic[cart_id] = []
        logger.info('new_cart')
        return cart_id


    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """

        with self.modify_cart:            
            check=False
            
            for i in range(len(self.products)):
                if self.products[i] == product:
                    check=True
            
            if check == False:
                return check

            self.producer_queue[self.producers_dic[product]] -= 1
            self.products.remove(product)

        self.cart_dic[cart_id].append(product)
        logger.info('add_to_cart')
        return True



    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        self.cart_dic[cart_id].remove(product)
        self.producer_queue.append(product)

        with self.modify_cart:
            self.producer_queue[self.producers_dic[product]] += 1
        logger.info('remove_from_cart')
        

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """

        for prod in self.cart_dic.pop(cart_id, None):
            with self.print_mutex:
                print("{} bought {}".format(currentThread().getName(), prod))

        logger.info('place_order')

        return self.cart_dic.pop(cart_id, None)
