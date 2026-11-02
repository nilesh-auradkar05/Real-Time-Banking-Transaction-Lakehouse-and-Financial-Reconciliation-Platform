\set ON_ERROR_STOP on
begin;

-- These foreign keys would reject the file order, delay checks
alter table fin_account alter constraint fin_account_district_id_fkey deferrable;
alter table fin_card alter constraint fin_card_disp_id_fkey deferrable;
alter table fin_client alter constraint fin_client_district_id_fkey deferrable;
alter table fin_disp alter constraint fin_disp_client_id_fkey deferrable;
alter table fin_disp alter constraint fin_disp_account_id_fkey deferrable;
set constraints  all deferred;

alter table fin_account alter data_origin set default 'czech_original';
alter table fin_card    alter data_origin set default 'czech_original';
alter table fin_client  alter data_origin set default 'czech_original';
alter TABLE fin_disp    alter data_origin set default 'czech_original';
alter TABLE fin_loan    alter data_origin set default 'czech_original';
alter TABLE fin_order   alter data_origin set default 'czech_original';

\copy fin_account (account_id, district_id, create_date, frequency) from 'fin_account.tsv' with (FORMAT text, null '')
\copy fin_card (card_id, disp_id, card_type, issued_date) from 'fin_card.tsv' with (FORMAT text, null '')
\copy fin_client (client_id, birth_date, gender, district_id) from 'fin_client.tsv' with (FORMAT text, null '')
\copy fin_disp (disp_id, client_id, account_id, disp_type) from 'fin_disp.tsv' with (FORMAT text, null '')
\copy fin_district (district_id, district_name, region, num_inhabitants, num_municipalities_gt499, num_municipalities_500to1999, num_municipalities_2000to9999, num_municipalities_gt10000, num_cities, ratio_urban, average_salary, unemployment_rate95, unemployment_rate96, num_entrep_per1000, num_crimes95, num_crimes96) from 'fin_district.tsv' with (FORMAT text, null '')
\copy fin_loan (loan_id, account_id, granted_date, amount, duration, payments, status) from 'fin_loan.tsv' with (FORMAT text, null '')
\copy fin_order (order_id, account_id, bank_to, account_to, amount, category) from 'fin_order.tsv' with (FORMAT text, null '')

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE fin_account ALTER data_origin DROP DEFAULT;
ALTER TABLE fin_card    ALTER data_origin DROP DEFAULT;
ALTER TABLE fin_client  ALTER data_origin DROP DEFAULT;
ALTER TABLE fin_disp    ALTER data_origin DROP DEFAULT;
ALTER TABLE fin_loan    ALTER data_origin DROP DEFAULT;
ALTER TABLE fin_order   ALTER data_origin DROP DEFAULT;
COMMIT;
