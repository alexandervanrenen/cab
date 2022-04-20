create table s_purchase_lineitem
(
    plin_purchase_id  bigint,
    plin_line_number  int,
    plin_item_id      varchar,
    plin_promotion_id varchar,
    plin_quantity     int,
    plin_sale_price   double precision,
    plin_coupon_amt   double precision,
    plin_comment      varchar
);

create table s_purchase
(
    purc_purchase_id   bigint,
    purc_store_id      varchar,
    purc_customer_id   varchar,
    purc_purchase_date varchar,
    purc_purchase_time int,
    purc_register_id   int,
    purc_clerk_id      int,
    purc_comment       varchar
);

create table s_catalog_order
(
    cord_order_id         bigint,
    cord_bill_customer_id varchar,
    cord_ship_customer_id varchar,
    cord_order_date       varchar,
    cord_order_time       int,
    cord_ship_mode_id     varchar,
    cord_call_center_id   varchar,
    cord_order_comments   varchar
);

create table s_web_order
(
    word_order_id         bigint,
    word_bill_customer_id varchar,
    word_ship_customer_id varchar,
    word_order_date       varchar,
    word_order_time       int,
    word_ship_mode_id     varchar,
    word_web_site_id      varchar,
    word_order_comments   varchar
);

create table s_catalog_order_lineitem
(
    clin_order_id            bigint,
    clin_line_number         int,
    clin_item_id             varchar,
    clin_promotion_id        varchar,
    clin_quantity            int,
    clin_sales_price         double precision,
    clin_coupon_amt          double precision,
    clin_warehouse_id        varchar,
    clin_ship_date           varchar,
    clin_catalog_number      int,
    clin_catalog_page_number int,
    clin_ship_cost           double precision
);

create table s_web_order_lineitem
(
    wlin_order_id     bigint,
    wlin_line_number  int,
    wlin_item_id      varchar,
    wlin_promotion_id varchar,
    wlin_quantity     int,
    wlin_sales_price  double precision,
    wlin_coupon_amt   double precision,
    wlin_warehouse_id varchar,
    wlin_ship_date    varchar,
    wlin_ship_cost    double precision,
    wlin_web_page_id  varchar
);

create table s_store_returns
(
    sret_store_id         varchar,
    sret_purchase_id      varchar,
    sret_line_number      int,
    sret_item_id          varchar,
    sret_customer_id      varchar,
    sret_return_date      varchar,
    sret_return_time      varchar,
    sret_ticket_number    varchar,
    sret_return_qty       int,
    sret_return_amt       double precision,
    sret_return_tax       double precision,
    sret_return_fee       double precision,
    sret_return_ship_cost double precision,
    sret_refunded_cash    double precision,
    sret_reversed_charge  double precision,
    sret_store_credit     double precision,
    sret_reason_id        varchar
);

create table s_catalog_returns
(
    cret_call_center_id     varchar,
    cret_order_id           int,
    cret_line_number        int,
    cret_item_id            varchar,
    cret_return_customer_id varchar,
    cret_refund_customer_id varchar,
    cret_return_date        varchar,
    cret_return_time        varchar,
    cret_return_qty         int,
    cret_return_amt         double precision,
    cret_return_tax         double precision,
    cret_return_fee         double precision,
    cret_return_ship_cost   double precision,
    cret_refunded_cash      double precision,
    cret_reversed_charge    double precision,
    cret_merchant_credit    double precision,
    cret_reason_id          varchar,
    cret_shipmode_id        varchar,
    cret_catalog_page_id    varchar,
    cret_warehouse_id       varchar
);

create table s_web_returns
(
    wret_web_page_id        varchar,
    wret_order_id           int,
    wret_line_number        int,
    wret_item_id            varchar,
    wret_return_customer_id varchar,
    wret_refund_customer_id varchar,
    wret_return_date        varchar,
    wret_return_time        varchar,
    wret_return_qty         int,
    wret_return_amt         double precision,
    wret_return_tax         double precision,
    wret_return_fee         double precision,
    wret_return_ship_cost   double precision,
    wret_refunded_cash      double precision,
    wret_reversed_charge    double precision,
    wret_account_credit     double precision,
    wret_reason_id          varchar
);

create table s_inventory
(
    invn_warehouse_id varchar,
    invn_item_id      varchar,
    invn_date         varchar,
    invn_qty_on_hand  int
);