#!/bin/bash
############################################################################
#Company: Sears Home Services
#Script: shs_datahub_srspos_pp_gcp_to_sf_load.sh
#version: 01
#author:Somdeb Mukherjee
#Purpose: This script serves as a datapipe between GCP to AWS Snowflake Via AWS S3.
#         This script is generic enough to be used with any Big Query Table.
#         Just replace the BQ Table name that you want to Bring to Snowflake in AWS. 
#         Note - In GCP you need to create your own DataSet. Please keep in mind the 
#                Cost Associated with it. Please Drop any data set after use. 
############################################################################
v_curr_tm=`date +'%Y-%m-%d %H:%M:%S:%3N'`
v_curr_tm_str=`date +'%Y%m%d%H%M%S%3N'`
v_curr_dt=`date +%Y-%m-%d`
UserId=`whoami`

v_sql_dir="/var/app/shs_datahub_prd_retail/sql"
v_data_directory="/var/app/shs_datahub_prd_retail/data"
v_data_archive_directory="/var/app/shs_datahub_prd_retail/data/archive"
v_log_dir="/var/app/shs_datahub_prd_retail/logs"
v_log_file_nm="srspos_pp_${v_curr_tm_str}_DLY.log"
v_s3_bucket_name="s3://shs-datahub-prd-retail/"
EMAIL_LIST="Somdeb.Mukherjee@transformco.com"

export v_strt_dt=$(date --date='-3day' '+%Y-%m-%d')
export v_end_dt=$(date '+%Y-%m-%d')

send_mail(){
HOST=`hostname`
SUBJECT=$1
MSG=$2
echo ${MSG} | mail -s "$SUBJECT" "$EMAIL_LIST"
}

check_ret_code(){
retVal=$1
proc_desc=$2
if [ $retVal -eq 0 ]
then
  echo "Success: ${proc_desc}" >> ${v_log_dir}/${v_log_file_nm}

else
  echo "Failure at Step: ${proc_desc}" >> ${v_log_dir}/${v_log_file_nm}
  send_mail "ALERT: POSXTMD_MK_DISC GCP to Snowflake Failure" "Processing failed at Step: ${proc_desc}"
  exit $retVal
fi
}

gcp_export(){
bq rm -f -t thc-shs-datahub-prod:Srs_Kmt_Pos_and_Retail_Data.SrsPOS_PP_Data >> ${v_log_dir}/${v_log_file_nm}
gsutil -m rm gs://gcp-preintegration-exports/srspos_pp_data/**
bq query --destination_table thc-shs-datahub-prod:Srs_Kmt_Pos_and_Retail_Data.SrsPOS_PP_Data --replace --parameter=v_strt_dt:DATE:"$v_strt_dt" --parameter=v_end_dt:DATE:"$v_end_dt" --use_legacy_sql=false 'Select Cus_Ian_Id_No, IAN_TYP_CD, Trs_Ln_No, Fty_Id_No, Reg_No, Trs_No, Trs_Dt, Trs_Tm, Ln_Itm_Trs_Typ_Cd, Div_Ogp_No, Prd_Itm_No, Cur_Div_Ogp_No, Cur_Prd_Itm_No, CUR_PRD_CHG_DT, Msc_Acn_No, Prd_Qt, Pos_Sls_Trs_Am, Itm_Rgl_Prc_Am, Plu_Am, Plu_Am_Typ_Cd, Ln_Itm_Tax_Am, Sku_No, Svc_Itm_Fl, Tot_Ln_Md_Am, Rtl_Div_No, Itl_Cd, Sll_Asc_Id_No, Prd_Inf_Typ_Cd, ORI_POS_DIV_OGP_NO, Null as LN_ITM_CNC_RSN_ID, Ln_Itm_Rsn_Cd, Ln_Itm_Sts_Cd, Ln_Itm_Sts_Rsn_Cd, Psd_Dt, Tax_Cd, Upc_No, Prc_Ovr_Cpn_No, Gft_Rcp_Prn_Fl, Bias_Itm_Fl, Rdc_Stk_Fl, Fll_Fr_Stk_Fl, Fll_Flr_Fl, Fll_Oth_Fl, Fly_By_Fl, Sht_Fl, Prd_Reg_Fl, Rdc_Ord_Fl, Bck_To_Str_Fl, Ma_Elg_Fl, Ppp_Elg_Fl, Rtl_Sp_Ord_Fl, Cto_Frn_Fl, Null as BIAS_ELG_ITM_FL, Null as Itl_Elg_Fl, Ddc_Fl, Ddl_Itm_Fl, Clr_Itm_Prc_Fl, Clr_Pnd_Fl, Prd_Rcl_Fl, Clr_And_Pro_Fl, Srs_Can_Tax1_Fl, Srs_Can_Tax2_Fl, Srs_Can_Tax3_Fl, Itm_Prt_Dl_Fl, Itm_Rsos_Ord_Fl, Bck_Str_Cnc_Fl, Itm_Inv_Fl, Itm_Rsk_Fee_Fl, Rsk_Fee_Am, Dos_Rtr_Rsn_Cd, Null as RTR_CNC_TYP_CD, Iias_Elig_Item_Fl, Benft_Typ_Cd, Mtch_Qt, Prc_Mtch_Am, Prc_Mtch_Bns_Am, Itm_Srl_No, Cut_To_Close_Ind, Competitor_Name, Ord_Type, Fulfill_Fty_Id_No, Pref_Fty_Id_No, Zip_Cd, Itm_Unq_Id_No, "GCP_TO_SF_PROCESS" as Creat_User_Id, SUBSTR(Cast(Load_Ts as String), 1, 19) as src_load_ts, Null as MOD_USER_ID, Null as MOD_TS From `shc-ent-data-library-prod.PreIntegration_Views.SrsPOS_PP_Data` Where (Cast(Load_Ts as Date) >= @v_strt_dt and Cast(Load_Ts as Date) <= @v_end_dt)' >> ${v_log_dir}/${v_log_file_nm}
bq extract --destination_format CSV --field_delimiter "|" --print_header=false 'Srs_Kmt_Pos_and_Retail_Data.SrsPOS_PP_Data' gs://gcp-preintegration-exports/srspos_pp_data/srspos_pp_data_export.*.csv >> ${v_log_dir}/${v_log_file_nm}
}

snowflake_load(){
snowsql -c prd_retail_gcp_int -r "SYSADMIN" -d "PRD_RETAIL" -s "BATCH" -f ${v_sql_dir}/shs_datahub_srspos_pp_load_ldts.sql -o exit_on_error=True -o friendly=False -o quiet=False -o timing=False -o header=false -o log_level=DEBUG >> ${v_log_dir}/${v_log_file_nm}
}

gcp_export
snowflake_load



