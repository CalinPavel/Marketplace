"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock
import unittest
import threading
import logging
from logging.handlers import RotatingFileHandler
from tema.product import Product, Tea

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('marketplace.log', maxBytes=0, backupCount=10)
logger.addHandler(handler)


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
        self.return_producer_id = Lock()
        self.producer_queue = []
        self.products = []
        self.producers_dic = {}
        self.create_new_cart = Lock()
        self.modify_cart = Lock()
        self.cart_count = 0
        self.cart_dic = {}
        self.print_mutex = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        logger.info('register_producer')

        with self.return_producer_id:
            self.producer_queue.append(0)
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

        prod_id = int(producer_id)

        if self.producer_queue[prod_id] >= self.queue_size_per_producer:
            return False

        self.producer_queue[prod_id] += 1
        self.products.append(product)
        self.producers_dic[product] = prod_id
        logger.info('publish')

        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        logger.info('new_cart')

        with self.create_new_cart:
            self.cart_count += 1
            cart_id = self.cart_count

        self.cart_dic[cart_id] = []
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
        logger.info('add_to_cart')

        with self.modify_cart:
            check = False

            for i in range(len(self.products)):
                if self.products[i] == product:
                    check = True

            if check is False:
                return check

            self.producer_queue[self.producers_dic[product]] -= 1
            self.products.remove(product)

        self.cart_dic[cart_id].append(product)
        return True

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """

        logger.info('remove_from_cart')

        try:
            with self.modify_cart:
                self.cart_dic[cart_id].remove(product)
                self.producer_queue.append(product)
                self.producer_queue[self.producers_dic[product]] += 1
                return True
        except NameError:
            return False

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """

        logger.info('place_order')
        for prod in self.cart_dic.pop(cart_id, None):
            with self.print_mutex:
                print(f"{threading.current_thread().name} bought {prod}")

        return self.cart_dic.pop(cart_id, None)


class TestMarketplace(unittest.TestCase):
    """Test marketplace."""

    def setUp(self):
        self.obj = Marketplace(3)

        self.pro_1 = Product('Wild Cherry', '5')
        self.tea_1 = Tea(self.pro_1, 'Black', '5')
        self.pro_2 = Product('Cactus fig', '3')
        self.tea_2 = Tea(self.pro_2, 'Green', '3')
        self.list = []
        self.list.append(self.tea_1)
        self.list.append(self.tea_2)

    def test_register_producer(self):
        """Register producer test."""
        self.assertEqual(self.obj.register_producer(), 0)
        self.assertEqual(self.obj.register_producer(), 1)
        self.assertEqual(self.obj.register_producer(), 2)

    def test_publish(self):
        """Publish product test."""
        self.obj.register_producer()
        self.assertEqual(self.obj.publish(0, self.pro_1), True)

    def test_new_cart(self):
        """Create new cart test."""
        self.assertEqual(self.obj.new_cart(), 1)
        self.assertEqual(self.obj.new_cart(), 2)

    def test_add_to_cart(self):
        """Add to cart test."""
        self.obj.register_producer()
        self.obj.publish(0, self.pro_1)
        self.assertEqual(self.obj.new_cart(), 1)
        self.assertEqual(self.obj.new_cart(), 2)
        self.assertEqual(self.obj.add_to_cart(1, self.pro_1), True)
        self.assertEqual(self.obj.add_to_cart(1, self.pro_2), False)

    def test_remove_from_cart(self):
        """Remove from cart test."""
        self.obj.register_producer()
        self.obj.publish(0, self.pro_1)
        self.assertEqual(self.obj.new_cart(), 1)
        self.assertEqual(self.obj.new_cart(), 2)
        self.assertEqual(self.obj.add_to_cart(1, self.pro_1), True)
        self.assertEqual(self.obj.remove_from_cart(1, self.pro_1), True)
        self.assertEqual(self.obj.remove_from_cart(5, self.pro_1), False)

    def test_place_order(self):
        """Place order test."""
        self.obj.register_producer()
        self.obj.publish(0, self.pro_1)
        self.assertEqual(self.obj.new_cart(), 1)
        self.assertEqual(self.obj.add_to_cart(1, self.pro_1), True)
        self.assertEqual(self.obj.place_order(1), None)


if __name__ == '__main__':
    unittest.main()
