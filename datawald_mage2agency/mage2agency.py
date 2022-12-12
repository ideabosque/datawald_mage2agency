# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from mage2_connector import Mage2Connector, Mage2OrderConnector


class Mage2Agency(Agency):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.mage2Connector = Mage2Connector(logger, **setting)
        self.mage2OrderConnector = Mage2OrderConnector(logger, **setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)

    def tx_asset_tgt(self, asset):
        return asset

    def tx_asset_tgt_ext(self, new_asset, asset):
        pass

    def insert_update_assets(self, assets):
        for asset in assets:
            tx_type = asset.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type == "product":
                    tgt_id = self.insert_update_product(asset)
                else:
                    raise Exception(f"TX Type ({tx_type}) is not supported!!!")

                asset.update(
                    {
                        "tx_status": "S",
                        "tx_note": f"datawald -> {asset['target']}",
                        "tgt_id": tgt_id,
                    }
                )
            except Exception:
                log = traceback.format_exc()
                asset.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(log)
        return assets

    def insert_update_product(self, asset):
        sku = asset.get("tx_type_src_id").replace(f"product-", "")
        product_id = self.mage2Connector.insert_update_product(
            sku,
            asset["data"].get("attribute_set", "default"),
            asset["data"],
            asset["data"].get("type_id"),
            asset["data"].get("store_id", 0),
        )
        if len(asset["data"].get("stock_data", {})) > 0:
            self.mage2Connector.insert_update_cataloginventory_stock_item(
                sku,
                asset["data"].get("stock_data"),
                asset["data"].get("store_id", 0),
            )

        if len(asset["data"].get("category_data", [])) > 0:
            self.mage2Connector.insert_update_categories(
                sku, asset["data"].get("category_data")
            )
        
        self.mage2Connector.insert_update_product_tier_price(
            sku,
            asset["data"].get("tier_price_data", []),
            asset["data"].get("store_id", 0),
        )

        if asset["data"].get("variant_data"):
            self.mage2Connector.insert_update_variant(
                sku,
                asset["data"].get("variant_data"),
                asset["data"].get("store_id", 0),
            )
        self.mage2Connector.request_magento_rest_api(
            api_path="integration/products/{sku}/urlkey".format(sku=sku), method="POST"
        )
        return product_id

    def insert_update_transactions(self, transactions):
        for transaction in transactions:
            tx_type = transaction.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type == "order":
                    tgt_id = self.insert_update_order(transaction)
                else:
                    raise Exception(f"TX Type ({tx_type}) is not supported!!!")

                transaction.update(
                    {
                        "tx_status": "S",
                        "tx_note": f"datawald -> {transaction['target']}",
                        "tgt_id": tgt_id,
                    }
                )
            except Exception:
                log = traceback.format_exc()
                transaction.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(log)
        return transactions

    def insert_update_order(self, transaction):
        # ecom_so = transaction.get("tx_type_src_id").replace(f"order-", "")
        # ecom_so_arr = ecom_so.split("-")
        # print(ecom_so)
        ecom_so = transaction["data"].get("ecom_so", None)
        if ecom_so is not None:
            increment_id = ecom_so
            type = "online_order"
            if len(increment_id.split("-")) == 1:
                processing_part = False
            else:
                increment_id = increment_id.split("-")[0]
                processing_part = True
            # tgt_id = self.insert_update_online_order(increment_id, transaction)
        else:
            increment_id = transaction["data"].get("so_number", None)
            type = "offline_order"
            processing_part = False
            # tgt_id = self.insert_update_offline_order(increment_id, transaction)
            
        order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
        if type == "offline_order" and order is None:
            self.insert_offline_order(increment_id, transaction)

        self.update_mage2_order(increment_id, transaction, type, processing_part)
        # if len(ecom_so_arr) > 0:
        #     if len(ecom_so_arr) == 2:
        #         increment_id = ecom_so_arr[0]
                
        #     elif len(ecom_so_arr) == 1:
        #         increment_id = ecom_so_arr[0]
                
        return ecom_so

    def update_mage2_order(self, increment_id, transaction, type, processing_part=False):
        tx_type_src_id = transaction.get("tx_type_src_id")
        items = transaction["data"].get("items", [])
        print(len(items))
        if len(items) == 0 or not increment_id:
            self.logger.error(f"{tx_type_src_id}: There is something wrong in data.")
            return
        order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
        if order is None:
            self.logger.error(f"{tx_type_src_id}: Can not find order")
            return
        print(increment_id)
        if transaction["data"].get("sales_rep_name", None):
            print(transaction["data"].get("sales_rep_name"))
            print("insert_order_comment")
            sales_rep_name_comment = "Sales Rep: {sales_rep_name}".format(sales_rep_name=transaction["data"].get("sales_rep_name"))
            print(sales_rep_name_comment)
            self.mage2OrderConnector.insert_order_comment(order_id=order["entity_id"], comment=sales_rep_name_comment, status=order.get("status"))

        order_ns_status = transaction["data"].get("status")
        print(order_ns_status)
        if self.mage2OrderConnector.can_invoice_order(order) and order_ns_status == "Billed":
            print("invoice")
            self.mage2OrderConnector.invoice_order(
            order_id=order.get("entity_id"),
            capture=True,
            items=[],
            notify=False,
            append_comment=False,
            comment=None,
            is_visible_on_front=False
            )
        print("check ship")
        print(self.mage2OrderConnector.can_ship_order(order))
        if self.mage2OrderConnector.can_ship_order(order) and order_ns_status == "Billed":
            print("ship")
            tracking_numbers = transaction["data"].get("tracking_numbers", [])
            print(tracking_numbers)
            if len(tracking_numbers) > 0:
                carrier_code = transaction["data"].get("carrier_code", "Carrier")
                tracks = [
                    {"carrier_code": carrier_code, "title": "Tracking Number", "track_number": track_number}
                    for track_number in tracking_numbers
                ]
                self.mage2OrderConnector.ship_order(
                    order_id=order.get("entity_id"),
                    items=[],
                    notify=False, 
                    append_comment=False,
                    comment=None,
                    is_visible_on_front=False,
                    tracks=tracks
                )

        transformed_status = self.transform_ns_order_status(transaction)
        print(transformed_status)
        if transformed_status is not None:
            order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
            current_order_status = order.get("status")
            ns_status = transformed_status.replace(" ","_").replace("-", "_").lower()
            current_order_state = order.get("state")
            print(current_order_status)
            print(ns_status)
            if current_order_status == ns_status:
                return
            if current_order_state in [self.mage2OrderConnector.STATE_NEW, self.mage2OrderConnector.STATE_PROCESSING,self.mage2OrderConnector.STATE_COMPLETE,self.mage2OrderConnector.STATE_CANCELED, self.mage2OrderConnector.STATE_CLOSED]:
                if current_order_state == self.mage2OrderConnector.STATE_CANCELED:
                    if ns_status == "canceled":
                        self.mage2OrderConnector.update_order_state_status(order.get("entity_id"), current_order_state, ns_status)
                    else:
                        self.mage2OrderConnector.update_order_state_status(order.get("entity_id"), self.mage2OrderConnector.STATE_PROCESSING, ns_status)
                else:
                    self.mage2OrderConnector.update_order_state_status(order.get("entity_id"), current_order_state, ns_status)
                
            status_comment = "Update Status to {ns_status}".format(ns_status=ns_status)
            print("insert_order_comment")
            self.mage2OrderConnector.insert_order_comment(
                order_id=order.get("entity_id"),
                comment=status_comment,
                status=ns_status,
                allow_duplicate_comment=True
            )
                    
        
    def transform_ns_order_status(self, transaction):
        status = None
        if transaction["data"].get("status") in ["canceled", "Cancelled"]:
            status = "canceled"
        elif transaction["data"].get("status") in ["closed", "Closed"]:
            status = "closed"
        else:
            if transaction["data"].get("hold_reason"):
                status = transaction["data"].get("hold_reason")
            if transaction["data"].get("fulfill_ship_status"):
                if_status = transaction["data"].get("fulfill_ship_status")
                suffix = ""
                if transaction["data"].get("carrier_code") == "CUSTOMER PICKUP":
                    suffix = "_for_customer"
                status = "{if_status}{suffix}".format(if_status=if_status, suffix=suffix)
        return status

    def insert_offline_order(self, increment_id, transaction):
        tx_type_src_id = transaction.get("tx_type_src_id")
        items = transaction["data"].get("items", [])
        if len(items) == 0 or not increment_id:
            self.logger.error(f"{tx_type_src_id}: No items or empty increment_id")
            return
        if not transaction["data"].get("customer_id"):
            raise Exception(f"{tx_type_src_id}: Empty customer_id")
        customer_id = self.get_customer_id_by_company_no(transaction["data"].get("customer_id"))
        if customer_id is None:
            raise Exception(f"{tx_type_src_id}: Can not find customer by company NO in Magento")
        customer_data = self.get_customer_data(customer_id)
        avaliable_items = []
        subtotal = 0
        shipping_amount = float(transaction["data"].get("shipping_amount", 0))
        total_qty_ordered = 0
        for item in items:
            product_id = self.mage2Connector.get_product_id_by_sku(item.get("sku"))
            if product_id != 0:
                avaliable_items.append(item)
                subtotal = subtotal + float(item.get("row_total"))
                total_qty_ordered = total_qty_ordered + float(item.get("qty_ordered", 0))
        grand_total = subtotal + shipping_amount
        billing_address = transaction["data"].get("billing_address", {})
        shipping_address = transaction["data"].get("shipping_address", {})
        posts = {
            "entity": {
                "state": "new",
                "status": "pending",
                "is_virtual": 0,
                "store_id": 1,
                "customer_id": customer_id,
                "base_grand_total": grand_total,
                "base_subtotal": subtotal,
                "grand_total": grand_total,
                "subtotal": subtotal,
                "shipping_amount": shipping_amount,
                "base_shipping_amount": shipping_amount,
                "total_qty_ordered": total_qty_ordered,
                "base_currency_code": "USD",
                "customer_email": customer_data["email"],
                "customer_firstname": customer_data["firstname"],
                "customer_lastname": customer_data["lastname"],
                "shipping_description": transaction["data"].get("carrier_code"),
                "store_currency_code": "USD",
                "increment_id": increment_id,
                "billing_address": {
                    "address_type": "billing",
                    "region": billing_address.get("region"),
                    "city": billing_address.get("city"),
                    "street": [billing_address.get("street")],
                    "postcode": billing_address.get("postcode"),
                    "firstname": billing_address.get("contact").split(" ")[0] if billing_address.get("contact") is not None else "",
                    "lastname": (" ".join(billing_address.get("contact").split(" ")[1:]) if len(billing_address.get("contact").split(" ")) > 1 else "") if billing_address.get("contact") is not None else "",
                    "company": billing_address.get("company"),
                    "country_id": billing_address.get("country_id"),
                    "telephone": billing_address.get("telephone", "######")
                },
                "items": [
                ],
                "payment": {
                    "method": self.setting.get("offline_default_payment_method", "checkmo")
                },
                "extension_attributes": {
                    "is_offline_order": True, 
                    "shipping_assignments": [
                        {
                            "shipping": {
                                "address": {
                                    "address_type": "shipping",
                                    "region": shipping_address.get("region"),
                                    "city": shipping_address.get("city"),
                                    "street": [shipping_address.get("street")],
                                    "postcode": shipping_address.get("postcode"),
                                    "firstname": shipping_address.get("contact").split(" ")[0] if shipping_address.get("contact") is not None else "",
                                    "lastname": (" ".join(shipping_address.get("contact").split(" ")[1:]) if len(shipping_address.get("contact").split(" ")) > 1 else "") if shipping_address.get("contact") is not None else "",
                                    "company": shipping_address.get("company"),
                                    "country_id": shipping_address.get("country_id"),
                                    "telephone": shipping_address.get("telephone", "######")
                                },
                                "method": self.setting.get("offline_default_shipping_method", "freeshipping_freeshipping")
                            }
                        }
                    ]
                }
            }
        }
        for avaliable_item in avaliable_items:
            posts["entity"]["items"].append(
                {
                    "sku": avaliable_item.get("sku"),
                    "is_virtual": 0,
                    "qty_ordered": avaliable_item.get("qty_orderd", 0),
                    "price": avaliable_item.get("price", 0),
                    "base_price": avaliable_item.get("price", 0),
                    "row_total": avaliable_item.get("row_total", 0),
                    "base_row_total": avaliable_item.get("row_total", 0)
                }
            )
        response = self.mage2OrderConnector.request_magento_rest_api(
            api_path="orders/create", method="PUT", payload=posts
        )
        print(response)
        pass
    
    def get_customer_id_by_company_no(self, company_no):
        try:
            (data_type, attribute_metadata) = self.mage2Connector.get_attribute_metadata("company_no", "customer")
        except Exception as e:
            return None
        attribute_id = attribute_metadata["attribute_id"]
        self.mage2Connector.adaptor.mysql_cursor.execute(
            "SELECT * FROM customer_entity_varchar WHERE attribute_id = %s AND value = %s;",
            [attribute_id, company_no]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        if res is None:
            return None
        else:
            return res["entity_id"]

    def get_customer_data(self, customer_id):
        self.mage2Connector.adaptor.mysql_cursor.execute(
            "SELECT * FROM customer_entity WHERE entity_id = %s;",
            [customer_id]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        return res
