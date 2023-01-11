# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback, json
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
        if setting.get("tx_type"):
            Agency.tx_type = setting.get("tx_type")

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
            ignore_category_ids = self.setting.get("ignore_category_ids", [])
            self.mage2Connector.insert_update_categories(
                sku, asset["data"].get("category_data"), ignore_category_ids
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

    def tx_transaction_tgt(self, transaction):
        return transaction

    def tx_transaction_tgt_ext(self, new_transaction, transaction):
        pass

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
        ecom_so = transaction["data"].get("ecom_so", None)
        warehouse = None
        if ecom_so is not None:
            increment_id = ecom_so
            type = "online_order"
            if len(increment_id.split("-")) == 2:
                warehouse = increment_id.split("-")[1]
                increment_id = increment_id.split("-")[0]
            # tgt_id = self.insert_update_online_order(increment_id, transaction)
        else:
            increment_id = transaction["data"].get("so_number", None)
            type = "offline_order"
            # tgt_id = self.insert_update_offline_order(increment_id, transaction)
            
        order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
        if type == "offline_order" and order is None:
            self.insert_offline_order(increment_id, transaction)
            self.mage2OrderConnector.adaptor.commit()

        tgt_id = self.update_mage2_order(increment_id, transaction, type, warehouse)
                
        return tgt_id

    def update_mage2_order(self, increment_id, transaction, type, warehouse=None):
        tx_type_src_id = transaction.get("tx_type_src_id")
        items = transaction["data"].get("items", [])
        if len(items) == 0 or not increment_id:
            self.logger.error(f"{tx_type_src_id}: There is something wrong in data.")
            return
        order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
        if order is None:
            self.logger.error(f"{tx_type_src_id}: Can not find order")
            return

        api_items=[]
        api_item_ids = []
        api_item_details = []
        if warehouse is not None:
            order_items = self.mage2OrderConnector.get_order_items(order.get("entity_id"))
            for ns_item in items:
                for order_item in order_items:
                    if order_item.get("parent_item_id", None) is None and order_item.get("sku") == ns_item.get("sku"):
                        product_options = json.loads(order_item.get("product_options")) if order_item.get("product_options", None) is not None else None
                        if product_options and product_options.get("info_buyRequest", {}).get("warehouse") == warehouse:
                            api_items.append({
                                "order_item_id": order_item.get("item_id"),
                                "qty": ns_item.get("qty_ordered", 0)
                            })
                            api_item_details.append(
                                {
                                    "order_item_id": order_item.get("item_id"),
                                    "sku": ns_item.get("sku"),
                                    "name": order_item.get("name"),
                                    "qty": ns_item.get("qty_ordered", 0)
                                }
                            )
                            api_item_ids.append(order_item.get("item_id"))
        if transaction["data"].get("sales_rep_name", None):
            sales_rep_name_comment = "Sales Rep: {sales_rep_name}".format(sales_rep_name=transaction["data"].get("sales_rep_name"))
            self.mage2OrderConnector.insert_order_comment(order_id=order["entity_id"], comment=sales_rep_name_comment, status=order.get("status"))

        order_ns_status = transaction["data"].get("status")
        if self.mage2OrderConnector.can_invoice_order(order) and order_ns_status == "Billed":
            can_invoice = True
            if len(api_item_ids) > 0 and not self.mage2OrderConnector.can_invoice_order_items(order, api_item_ids):
                can_invoice = False
            if can_invoice:
                append_comment = False
                comment = None
                if len(api_items) > 0:
                    append_comment = True
                    comment= "Invoiced {details}".format(
                        details=", ".join([
                            "{name}({sku})".format(name=item.get("name"), sku=item.get("sku"))
                            for item in api_item_details
                        ])
                    )
                self.mage2OrderConnector.invoice_order(
                    order_id=order.get("entity_id"),
                    capture=True,
                    items=api_items,
                    notify=False,
                    append_comment=append_comment,
                    comment=comment,
                    is_visible_on_front=False
                )
                self.mage2OrderConnector.adaptor.commit()

        if self.mage2OrderConnector.can_ship_order(order) and order_ns_status == "Billed":
            can_ship = True
            if len(api_item_ids) > 0 and not self.mage2OrderConnector.can_ship_order_items(order, api_item_ids):
                can_ship = False
            tracking_numbers = transaction["data"].get("tracking_numbers", [])
            if len(tracking_numbers) > 0 and can_ship:
                carrier_code = transaction["data"].get("carrier_code", "Carrier")
                tracks = [
                    {"carrier_code": carrier_code, "title": "Tracking Number", "track_number": track_number}
                    for track_number in tracking_numbers
                ]
                append_comment = False
                comment = None
                if len(api_items) > 0:
                    append_comment = True
                    comment= "Shipped {details}".format(
                        details=", ".join([
                            "{name}({sku})".format(name=item.get("name"), sku=item.get("sku"))
                            for item in api_item_details
                        ])
                    )
                self.mage2OrderConnector.ship_order(
                    order_id=order.get("entity_id"),
                    items=api_items,
                    notify=False, 
                    append_comment=append_comment,
                    comment=comment,
                    is_visible_on_front=False,
                    tracks=tracks
                )
                self.mage2OrderConnector.adaptor.commit()

        transformed_status = self.transform_ns_order_status(transaction)
        if transformed_status is not None:
            if warehouse is not None:
                return
            order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
            current_order_status = order.get("status")
            ns_status = transformed_status.replace(" ","_").replace("-", "_").lower()
            current_order_state = order.get("state")
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
            self.mage2OrderConnector.insert_order_comment(
                order_id=order.get("entity_id"),
                comment=status_comment,
                status=ns_status,
                allow_duplicate_comment=True
            )
        return order.get("entity_id")
                    
        
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
                    suffix = "_for_cusotmer"
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
                type_id = self.mage2Connector.get_product_type_id_by_sku(item.get("sku"))
                product_name = self.mage2Connector.get_entity_attribute_value("catalog_product", product_id, "name", store_id=0)
                weight = self.mage2Connector.get_entity_attribute_value("catalog_product", product_id, "weight", store_id=0)
                avaliable_items.append(dict(item, **{"product_id": product_id, "product_type": type_id, "product_name": product_name, "weight": weight}))
                subtotal = subtotal + float(item.get("row_total"))
                total_qty_ordered = total_qty_ordered + float(item.get("qty_ordered", 0))
        if len(avaliable_items) == 0:
            raise Exception(f"{tx_type_src_id}: No avaliable product items")
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
                    "amasty_order_attributes": [
                        {
                            "attribute_code": "is_offline_order",
                            "value": True
                        }
                    ], 
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
                    "product_id": avaliable_item.get("product_id"),
                    "product_type": avaliable_item.get("product_type"),
                    "name": avaliable_item.get("product_name"),
                    "weight": str(avaliable_item.get("weight", 0)),
                    "store_id": 1,
                    "is_virtual": 0,
                    "qty_ordered": avaliable_item.get("qty_ordered", 0),
                    "price": avaliable_item.get("price", 0),
                    "base_price": avaliable_item.get("price", 0),
                    "row_total": avaliable_item.get("row_total", 0),
                    "base_row_total": avaliable_item.get("row_total", 0)
                }
            )
        response = self.mage2OrderConnector.request_magento_rest_api(
            api_path="orders/create", method="PUT", payload=posts
        )
    
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
        res = self.mage2Connector.adaptor.mysql_cursor.fetchone()
        if res is None:
            return None
        else:
            return res["entity_id"]

    def get_customer_data(self, customer_id):
        self.mage2Connector.adaptor.mysql_cursor.execute(
            "SELECT * FROM customer_entity WHERE entity_id = %s;",
            [customer_id]
        )
        res = self.mage2Connector.adaptor.mysql_cursor.fetchone()
        return res
