
update
  t_cnt_tm_tariffcomponent
set
  tariffcompexpression = 'IIF(Consumer Registered == Yes,IIF(Net Metering == Yes,IIF((Gross Active Energy Consumption-Net Export Active Energy)<=200*Total Billing Period,(Sundry Fuel Adjustment Charges+Special Fuel Surcharge)*-1,0),IIF(Total Gross Active Energy Consumption <=200*Total Billing Period,(Sundry Fuel Adjustment Charges+Special Fuel Surcharge)*-1,0)),0)',
  tariffencodcompexp = '$IIF(#251==1,$IIF(#247==1,$IIF((@47-@149)<=200*#84,(#161+#227)*-1,0),$IIF(#249<=200*#84,(#161+#227)*-1,0)),0)'
where
  tariffcompmasterid = '130'
  and tariffrefno in (
    '541','542','543','544','545','546','547','548','549','550','551','552','553','554','555','677','678','679','680','681','682','689',
    '690','691','612','613',
    '614','687','688'
  );

---------------------------------------------------------------------------------------------------------


  -- FUNCTION: billing.fn_cnt_bl_savedefferedfuelsundry(character varying, numeric, numeric, numeric)

-- DROP FUNCTION IF EXISTS billing.fn_cnt_bl_savedefferedfuelsundry(character varying, numeric, numeric, numeric);

CREATE OR REPLACE FUNCTION billing.fn_cnt_bl_savedefferedfuelsundry(
	pconnectionno character varying,
	ptransactionno numeric,
	pbillno numeric,
	pspecialfuel numeric)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
v_transactionid bigint;

BEGIN

	select nextval('t_cnt_bl_enteradjustcharge_transactionid_seq') into v_transactionid;

	with consdata AS 
	(
		select gaaid as gaacode,maincategoryid,consumernumber as consumerno, connectionno 
		from t_cnt_cim_consumer where connectionno=pconnectionno
	)	
	,sundrydata as(				
		SELECT bladjust.billcomponentid,consumerno,connectionno,adjcategoryid,
		pspecialFuel billedamount, 
		billadjustmentid, adjustmentcode,
		adjustmentdescription,gaacode
		from consdata 
		inner join t_cnt_bl_billadjustmenttype bladjust 
		on bladjust.billcomponentid=48 and adjustmentcode=120 and  bladjust.adjcategoryid=0                  
		and sundrynature='Assessment' and sundryformcode= 7	 
	),
	adjustcomponent as (
	INSERT INTO t_cnt_bl_adjustmentcomponent(componentamount, billcomponentid, transactionid, isdeleted, billadjustmentid, 
	createdby, creationdatetime, lastmodifiedby, lastmodifieddatetime)
	select  -billedamount , billcomponentid, v_transactionid,0,
	billadjustmentid, 'fn_cnt_bl_savedefferedFuelSundry',localtimestamp, null, null 
	from sundrydata)
	INSERT INTO t_cnt_bl_enteradjustcharge(consumernumber, connectionnumber,transactionid,vouchernumber,adjustmentcode, remark, lastmodifiedby, lastmodifieddatetime, 
	createdby, creationdatetime, isdeleted, totaladjustmentamount,billaccounted,connectioncategory, billno,
	gaaid, isautomatic)
	select consumerno, connectionno,v_transactionid,ptransactionno,adjustmentcode,adjustmentdescription, null , null, 	'fn_cnt_bl_savedefferedFuelSundry', localtimestamp,
	0,-billedamount ,1, adjcategoryid,pbillno,gaacode, 1 from sundrydata;

return true;
end;
$BODY$;

ALTER FUNCTION billing.fn_cnt_bl_savedefferedfuelsundry(character varying, numeric, numeric, numeric)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION billing.fn_cnt_bl_savedefferedfuelsundry(character varying, numeric, numeric, numeric) TO PUBLIC;

GRANT EXECUTE ON FUNCTION billing.fn_cnt_bl_savedefferedfuelsundry(character varying, numeric, numeric, numeric) TO billingadmin;

GRANT EXECUTE ON FUNCTION billing.fn_cnt_bl_savedefferedfuelsundry(character varying, numeric, numeric, numeric) TO postgres;

GRANT EXECUTE ON FUNCTION billing.fn_cnt_bl_savedefferedfuelsundry(character varying, numeric, numeric, numeric) TO reportingadmin;

---------------------------------------------------------------------------------------------------------

-- FUNCTION: billing.fn_cnt_billing_updatebalanceamtforrevisebill(numeric, character varying, character varying, integer)

-- DROP FUNCTION IF EXISTS billing.fn_cnt_billing_updatebalanceamtforrevisebill(numeric, character varying, character varying, integer);

CREATE OR REPLACE FUNCTION billing.fn_cnt_billing_updatebalanceamtforrevisebill(
	pbillno numeric,
	pconnectionno character varying,
	pusername character varying,
	pauditactivity integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
rec_billedcomponent record;
--rec_balanceData record;
--v_amount numeric;
v_transactionno integer;
v_categorycode character varying;
v_isfortnightlybill integer;
--v_componentid integer;
vLastFNBDues numeric;
vLastFNBDuesPaid numeric;
v_isnetmetering integer;
v_iscurrentamountrevert integer;
v_billmonthid integer;
v_billyear integer;
v_voucherno character varying;
v_provisionalFuelsundry numeric;
v_defferedFuelsundry numeric;
BEGIN

v_provisionalFuelsundry:=0;
v_defferedFuelsundry:=0;
SELECT transactionno,categorycode,iscurrentamountrevert, billmonthid, billyear FROM t_cnt_bl_billmaster WHERE billno = pbillno
INTO v_transactionno,v_categorycode,v_iscurrentamountrevert, v_billmonthid, v_billyear;

--To be deleted after 042021 cycle
if(v_billmonthid=4 and v_billyear=2021) then
	update t_cnt_bl_enteradjustcharge adj
	set isdeleted=1	
	from t_cnt_bl_billmaster bill	
	where adj.connectionnumber = bill.connectionno	
	and billmonthid=4 and billyear=2021
	and adjustmentcode=23 and connectionnumber=pconnectionno
	and billtypecriterianame='Migration' and generatedby='ETL'
	and vouchernumber = ('CDF_'|| bill.billno) and adj.isdeleted=0;   
	
end if;
--To be deleted after 042021 cycle

SELECT isfortnightlybill,isnetmetering FROM t_cnt_cim_consumer
WHERE connectionno = pconnectionno
INTO v_isfortnightlybill,v_isnetmetering;

-- Mark solar sundry delete 
if(v_isnetmetering=1)
then 
	UPDATE  T_CNT_BL_ENTERADJUSTCHARGE SET isdeleted =1,lastmodifiedby='Net Meter Bill Canceled' ,  lastmodifieddatetime= now() WHERE  BILLNO=pBILLNO and 
	isautomatic=1 and adjustmentcode=72 and connectionnumber=pconnectionno;
end if ;

if(v_categorycode='72')
then 
	UPDATE  T_CNT_BL_ENTERADJUSTCHARGE SET isdeleted =1,lastmodifiedby='DBT Bill canceled' ,  lastmodifieddatetime= now() WHERE  BILLNO=pBILLNO and 
	isautomatic=1 and adjustmentcode=51 and connectionnumber=pconnectionno;
	
	UPDATE  t_cnt_bl_DBTSundryData set  isdeleted=1, modifiedon =now()  ,
	modifiedby ='fn_cnt_billing_updatebalanceamtforrevisebill' where billno=pbillno and connectionno=pconnectionno;
end if ;

SELECT eac.transactionid into v_provisionalFuelsundry
		from T_CNT_BL_ENTERADJUSTCHARGE eac 
		inner join t_cnt_bl_billadjustmenttype bladjust 
		on bladjust.billcomponentid=48 and bladjust.adjustmentcode=(case v_categorycode when '69' then 121 else 100 end) 
		and  bladjust.adjcategoryid=(case v_categorycode when '69' then 69 else 0 end)                  
		and sundrynature='Assessment' and sundryformcode=(case v_categorycode when '69' then 4 else 6 end)	
		where billno=pbillno and eac.adjustmentcode=(case v_categorycode when '69' then 121 else 100 end)
		and connectionnumber=pconnectionno and eac.isautomatic=1;

if(v_provisionalFuelsundry <> 0)then
	UPDATE  T_CNT_BL_ENTERADJUSTCHARGE SET isdeleted =1,lastmodifiedby='Provisional Fuel Bill canceled' ,  lastmodifieddatetime= now() WHERE  BILLNO=pBILLNO and 
	isautomatic=1 and transactionid=v_provisionalFuelsundry and connectionnumber=pconnectionno;	
end if;

SELECT eac.transactionid into v_defferedFuelsundry
		from T_CNT_BL_ENTERADJUSTCHARGE eac 
		inner join t_cnt_bl_billadjustmenttype bladjust 
		on bladjust.billcomponentid=48 and bladjust.adjustmentcode=120 
		and  bladjust.adjcategoryid=0                 
		and sundrynature='Assessment' and sundryformcode=7
		where billno=pbillno and eac.adjustmentcode=120
		and connectionnumber=pconnectionno and eac.isautomatic=1;

if(v_defferedFuelsundry <> 0)then
	UPDATE  T_CNT_BL_ENTERADJUSTCHARGE SET isdeleted =1,lastmodifiedby='Deferred Fuel Bill canceled' ,  lastmodifieddatetime= now() WHERE  BILLNO=pBILLNO and 
	isautomatic=1 and transactionid=v_defferedFuelsundry and connectionnumber=pconnectionno;	
end if;

if(v_iscurrentamountrevert=0)
then 
update t_Cnt_bl_billmaster set iscurrentamountrevert=1 where  transactionno=v_transactionno;
FOR rec_billedcomponent IN

select sum (billedamount*mf) amount ,componentid from t_cnt_bl_billedcomponents bc inner join t_cnt_bl_revisebalaceamountmaster rbm on 
bc.billedcomponentid=subcomponentid where isactive=1 and transactionno=v_transactionno and finalbilldatarefno = pbillno
--and componentid==rec_billedcomponent.billedcomponentid 
group by componentid

                /*SELECT  billedcomponentid, billedamount FROM t_cnt_bl_billedcomponents billedcomponent 
                where transactionno=v_transactionno and finalbilldatarefno = pbillno and billedcomponent.billedcomponentid in (54,55,56,58,61,134)*/
LOOP     
update t_cnt_bl_Balanceamount set balanceamount=balanceamount+rec_billedcomponent.amount,
				modifiedon=CURRENT_TIMESTAMP, 
                                modifiedby=pusername,
                                auditactivity = pauditactivity 
                                WHERE  connectionNo=pconnectionno AND billcomponentid=rec_billedcomponent.componentid;

/*
if(rec_billedcomponent.billedcomponentid = 58 ) THEN 
v_componentid:=45;
elsif (rec_billedcomponent.billedcomponentid = 54 ) THEN 
v_componentid=34;
elsif(rec_billedcomponent.billedcomponentid = 55 ) THEN 
v_componentid:=35;
elsif(rec_billedcomponent.billedcomponentid = 56 ) THEN 
v_componentid=36;
elsif(rec_billedcomponent.billedcomponentid = 61 ) THEN 
v_componentid:=61;
elsif(rec_billedcomponent.billedcomponentid = 134 ) THEN 
v_componentid=78;
end if;

UPDATE t_cnt_bl_Balanceamount SET balanceamount = rec_billedcomponent.billedamount, 
                                modifiedon=CURRENT_TIMESTAMP, 
                                modifiedby=pusername,
                                auditactivity = pauditactivity 
                                WHERE  connectionNo=pconnectionno AND billcomponentid=v_componentid;*/

END LOOP;                             

IF(v_isfortnightlybill = 1 ) THEN

	--187;"Last FNB Nigam Dues"
	--191 paid amount
	select sum(case when parameterid=187 then value else 0 end) LastFNBDues, sum(case when parameterid=191 then value else 0 end) LastFNBDuesPaid 
	from t_cnt_bl_connbillparameterdata where transactionno =v_transactionno and parameterid in (191,187)
	INTO vLastFNBDues,vLastFNBDuesPaid;   

	--fnb amount
	UPDATE t_cnt_bl_Balanceamount                              
	  SET balanceamount = vLastFNBDues -  vLastFNBDuesPaid, modifiedon=CURRENT_TIMESTAMP, modifiedby= pusername,auditactivity = pauditactivity
	   WHERE   connectionno=pconnectionno AND billcomponentid = 121;
	--fnb dues
	UPDATE t_cnt_bl_Balanceamount SET balanceamount =  vLastFNBDuesPaid, modifiedon=CURRENT_TIMESTAMP, modifiedby= pusername,auditactivity = pauditactivity
	WHERE   connectionno=pconnectionno AND billcomponentid = 122;        

	UPDATE t_cnt_bl_Balanceamount SET balanceamount =  balanceamount -vLastFNBDuesPaid, modifiedon=CURRENT_TIMESTAMP, modifiedby= pusername,auditactivity = pauditactivity
	WHERE   connectionno=pconnectionno AND billcomponentid = 45;          
		   
END IF;

if(v_categorycode='69') then
begin
with get_Bill as (
select Connectionno, duedate, bm.billno, billmonthid, billyear, bm.transactionno,islpsrecalculated, veestatus,categorycode::int
from billing.t_cnt_bl_billmaster bm
where isfortnightlybill = 'f'
and 
categorycode::int =69 -- LIP
and  bm.transactionno=v_transactionno and  islpsrecalculated=1
)
, billdata_forLPS as (
select b.categorycode,b.Connectionno, b.transactionno, duedate,billno, billmonthid, billyear, coalesce(blps.Billedamount,0) bill_lpsc, coalesce(boldlps.Billedamount,0) bill_oldarrear_lps
from get_Bill b , t_cnt_bl_billedcomponents blps, t_cnt_bl_billedcomponents boldlps
where b.transactionno = blps.transactionno 
and blps.billedcomponentid=42 -- LPS from bill
and b.transactionno = boldlps.transactionno 
and boldlps.billedcomponentid=123 -- Old arrear LPS
)
update billing.t_cnt_bl_balanceamount bm set balanceamount = bm.balanceamount- bill_lpsc - bill_oldarrear_lps ,
 modifiedon=CURRENT_TIMESTAMP, modifiedby= pusername,auditactivity = pauditactivity
--select bm.Connectionno, balanceamount,bill_lpsc , bill_oldarrear_lps,  bill_lpsc + bill_oldarrear_lps 
from billdata_forLPS cal--, billing.t_cnt_bl_balanceamount bm 
where bm.Connectionno = cal.Connectionno
and bm.billcomponentid=61 -- Arrear LPS 
and categorycode=69
;
end;
else

update billing.t_cnt_bl_balanceamount bm set balanceamount = bm.balanceamount-lpsamount ,
 modifiedon=CURRENT_TIMESTAMP, modifiedby= pusername,auditactivity = pauditactivity
from (
select Connectionno,bc.billedamount lpsamount
from billing.t_cnt_bl_billmaster bm inner join t_cnt_bl_Billedcomponents bc on bm.transactionno=bc.transactionno
where 
  bm.transactionno=v_transactionno and  islpsrecalculated=1 and bc.billedcomponentid=42 and categorycode::int!=69
  )  cal
  where bm.Connectionno = cal.Connectionno
and bm.billcomponentid=61 ;-- Arrear LPS 

end if;
end if;

return 1;

END;
$BODY$;

ALTER FUNCTION billing.fn_cnt_billing_updatebalanceamtforrevisebill(numeric, character varying, character varying, integer)
    OWNER TO billingadmin;

GRANT EXECUTE ON FUNCTION billing.fn_cnt_billing_updatebalanceamtforrevisebill(numeric, character varying, character varying, integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION billing.fn_cnt_billing_updatebalanceamtforrevisebill(numeric, character varying, character varying, integer) TO billingadmin;

GRANT EXECUTE ON FUNCTION billing.fn_cnt_billing_updatebalanceamtforrevisebill(numeric, character varying, character varying, integer) TO reportingadmin;

