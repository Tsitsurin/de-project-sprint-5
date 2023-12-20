
create table if not exists cdm.dm_courier_ledger(
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
courier_id varchar NOT NULL,
courier_name varchar NOT NULL,
settlement_year int NOT NULL,
settlement_month int NOT NULL,
orders_count integer NOT NULL,
orders_total_sum numeric(14, 2) NOT NULL,
rate_avg numeric(14, 2) NOT NULL,
order_processing_fee numeric(14, 2) NOT NULL,
courier_order_sum numeric(14, 2) NOT null,
courier_tips_sum numeric(14, 2) NOT null,
courier_reward_sum numeric(14, 2) NOT NULL
);



ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN orders_count SET default 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN orders_total_sum SET default 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN rate_avg SET default 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN order_processing_fee SET default 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN courier_order_sum SET default 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN courier_tips_sum SET default 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN courier_reward_sum SET default 0;


ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum >= (0)::numeric);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_orders_rate_avg_check CHECK (rate_avg >= (0)::numeric);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK (order_processing_fee >= (0)::numeric);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK (courier_order_sum >= (0)::numeric);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK (courier_tips_sum >= (0)::numeric);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK (courier_reward_sum >= (0)::numeric);


drop table if exists dds.fct_deliveries cascade;
drop table if exists dds.dm_deliveries;
drop table if exists dds.dm_couriers;
drop table if exists dds.dm_orders;

CREATE TABLE IF NOT EXISTS dds.dm_orders(
	order_id_dwh serial PRIMARY key,
	order_id_source	varchar(30) UNIQUE
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers(
	courier_id_dwh serial PRIMARY KEY,
	courier_id_source varchar(30) UNIQUE,
	name varchar
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries(
	delivery_id_dwh serial PRIMARY KEY,
	delivery_id_source varchar(30) UNIQUE
);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries(
    order_id_dwh integer PRIMARY key,
	delivery_id_dwh	integer,
	courier_id_dwh	integer,
	order_ts timestamp,
    delivery_ts timestamp,
	address varchar,
	rate  integer,
	tip_sum numeric (14, 2),
	total_sum numeric (14, 2),

    CONSTRAINT fct_deliveries_order_id_dwh_fkey
    FOREIGN KEY (order_id_dwh)
    REFERENCES dds.dm_orders(order_id_dwh),

    CONSTRAINT fct_deliveries_delivery_id_dwh_fkey
    FOREIGN KEY (delivery_id_dwh)
    REFERENCES dds.dm_deliveries(delivery_id_dwh),

    CONSTRAINT fct_deliveries_courier_id_dwh_fkey
    FOREIGN KEY (courier_id_dwh)
    REFERENCES dds.dm_couriers(courier_id_dwh)
);


CREATE TABLE IF NOT EXISTS stg.couriers (
_id text not null,
name text not null
);

CREATE TABLE IF NOT EXISTS stg.deliveries (
order_id text NOT NULL,
order_ts timestamp NOT NULL,
delivery_id text NOT NULL,
courier_id text NOT NULL,
address text NOT NULL,
delivery_ts timestamp NOT NULL,
rate smallint NOT NULL,
sum numeric(14, 2) NOT NULL,
tip_sum numeric(14, 2) NOT NULL
);