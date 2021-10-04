/*******************************************************************************
** Goal: 
	Pre-process NHATS-CMS inpatient claims data and merge post-hospitalization 
	(SNF/IRF/HHA) info onto inpatient hospitalization data
*******************************************************************************/

/*******************************************************************************
*** WAVE 1
*******************************************************************************/
* import NHATS-CMS inpatient claims;
filename ipt2010 "/PATH/inpatient_base_claims_j_read_v8_2010.sas";
%include ipt2010;

filename ipt2011 "/PATH/inpatient_base_claims_j_read_v8_2011.sas";
%include ipt2011;

filename ipt2012 "/PATH/inpatient_base_claims_j_read_v8_2012.sas";
%include ipt2012;

filename ipt2013 "/PATH/inpatient_base_claims_j_read_v8_2013.sas";
%include ipt2013;

filename ipt2014 "/PATH/inpatient_base_claims_j_read_v8_2014.sas";
%include ipt2014;

* combine all inpatient claims for Wave 1;
data wave1_inpatient;
	set inpatient_base_claims_j_2010
		inpatient_base_claims_j_2011
		inpatient_base_claims_j_2012
		inpatient_base_claims_j_2013
		inpatient_base_claims_j_2014;
	
	* extract discharge year and month;
	dschrg_year = year(NCH_BENE_DSCHRG_DT);
	dschrg_month = month(NCH_BENE_DSCHRG_DT);
	los_inpatient = NCH_BENE_DSCHRG_DT - CLM_ADMSN_DT;
	keep BENE_ID PRVDR_NUM NCH_CLM_TYPE_CD NCH_PTNT_STATUS_IND_CD PTNT_DSCHRG_STUS_CD 
		CLM_ADMSN_DT NCH_BENE_DSCHRG_DT dschrg_year dschrg_month los_inpatient CLM_DRG_CD;
run; *9978 obs;

* filter inpatient hospitalization;
data wave1_inp_hosp;
	set wave1_inpatient;
	if (substr(PRVDR_NUM,3,1) = '0' or substr(PRVDR_NUM,3,2) = '13') /* restrict to claims from GAC and CAH */
		and NCH_CLM_TYPE_CD in ("60", "61", "62", "63", "64")		/* restrict to inpatient claim type 60-64; */
		and NCH_PTNT_STATUS_IND_CD = "A";		/* restrict to discharge status = discharged alive (as opposed to died or still a patient)*/
run; * 9067 obs; 

* Get unique record for each BENE_ID on same discharge date;
proc sort data = wave1_inp_hosp;
	by BENE_ID NCH_BENE_DSCHRG_DT;
run;

data wave1_inp_hosp;
	set wave1_inp_hosp;
	by BENE_ID NCH_BENE_DSCHRG_DT;
	if first.NCH_BENE_DSCHRG_DT then output;
run; * 9063 obs;

* merge with MBSF + MBSF part D data;
proc sql;
	create table merged_wave1 as
	select *
	from (
		select *
		from wave1_inp_hosp as a
			left join wave1_mbsf as b
			on a.BENE_ID = b.BENE_ID) as c
		left join wave1_mbsf_d (keep = BENE_ID DUAL:) as d
		on c.BENE_ID = d.BENE_ID;			
quit;

%checkmiss(merged_wave1)

* determine part A, part B, and MA status from admission to post discharge;
* add dual eligibility;
data merged_wave1_inpatient;
	set merged_wave1;
	array buyin{60} BUYIN1 - BUYIN60;
	array hmoin{60} HMOIND1 - HMOIND60;
	array elig_period{60} elig_period_1 - elig_period_60;

	array enroll_indicator{60} enroll_indicator_1 - enroll_indicator_60;
	array elig_indicator{60} elig_indicator_1 - elig_indicator_60;
	array MA_indicator(60) MA_indicator_1 - MA_indicator_60;
	array MA_elig_indicator(60) MA_elig_indicator_1 - MA_elig_indicator_60;
	array FFS_indicator(60) FFS_indicator_1 - FFS_indicator_60;
	array FFS_elig_indicator(60) FFS_elig_indicator_1 - FFS_elig_indicator_60;
	
	array dual{60} DUAL1 - DUAL60;
	array dual_indicator{60} dual_indicator_1 - dual_indicator_60;
	array dual_elig_indicator{60} dual_elig_indicator_1 - dual_elig_indicator_60;
	
	if DEATH_DT ne . & DEATH_DT ge NCH_BENE_DSCHRG_DT 
		then post_dischdt = min(NCH_BENE_DSCHRG_DT + 90 , DEATH_DT); 
		else post_dischdt = NCH_BENE_DSCHRG_DT + 90;
	
	do i = 1 to 60;
		
	* eligible period;
	if (year(CLM_ADMSN_DT) - 2010) * 12 + month(CLM_ADMSN_DT) le i le (year(post_dischdt) - 2010) * 12 + month(post_dischdt) 
		then elig_period(i) = 1; 
    	else elig_period(i) = 0; 
    	
	* Eligible for Part A & B coverage;
	if buyin(i) in ('3','C') 
		then enroll_indicator(i) = 1; 
		else enroll_indicator(i) = 0;
		    	
    elig_indicator(i) = enroll_indicator(i) * elig_period(i);
    
	*Eligible for Part A & B - enrolled in MA;
	if buyin(i) in ('3','C') & hmoin(i) not in (' ','0','4') 
		then MA_indicator(i) = 1; 
		else MA_indicator(i) = 0;
		
	MA_elig_indicator(i) = MA_indicator(i) * elig_period(i);
	
	*Eligible for Part A & B - enrolled in FFS;
	if buyin(i) in ('3','C') & hmoin(i) in (' ','0','4') 
		then FFS_indicator(i) = 1; 
		else FFS_indicator(i) = 0;
		
	FFS_elig_indicator(i) = FFS_indicator(i) * elig_period(i);
	
	* dual eligible;
	if dual(i) in ('02', '04', '08')
		then dual_indicator(i) = 1;
		else dual_indicator(i) = 0;
	
	dual_elig_indicator(i) = dual_indicator(i) * elig_period(i);
	end; 

	drop i;
	elig_sum = sum(of elig_indicator_1-elig_indicator_60);
	MA_elig_sum = sum(of MA_elig_indicator_1-MA_elig_indicator_60);
	FFS_elig_sum = sum(of FFS_elig_indicator_1-FFS_elig_indicator_60);
	dual_elig_sum = sum(of dual_elig_indicator_1-dual_elig_indicator_60);
	
	*Continuously eligible for Part A & B;
	if elig_sum eq (year(post_dischdt) - year(CLM_ADMSN_DT)) * 12 + month(post_dischdt) - month(CLM_ADMSN_DT) + 1 
		then enrollment_elig = 1; 
		else enrollment_elig = 0;

	*Continuously eligible for Part A & B - continuously enrolled in MA;
	if MA_elig_sum eq (year(post_dischdt) - year(CLM_ADMSN_DT)) * 12 + month(post_dischdt) - month(CLM_ADMSN_DT) + 1 
		then MA_enrollment_elig = 1; 
		else MA_enrollment_elig = 0;

	*Continuously eligible for Part A & B - continuously enrolled in FFS;
	if FFS_elig_sum eq (year(post_dischdt) - year(CLM_ADMSN_DT)) * 12 + month(post_dischdt) - month(CLM_ADMSN_DT) + 1 
		then FFS_enrollment_elig = 1;
		else FFS_enrollment_elig = 0;
		
	*Ever dual eligible;
	if dual_elig_sum > 0
		then dual_eligible = 1;
		else dual_eligible = 0;

	label MA_enrollment_elig="Continuous Enrollment in Medicare Advantage" 
    	  FFS_enrollment_elig="Continuous Enrollment in Fee-for-Service";
    	  
    drop BUYIN: HMOIND: DUAL1 - DUAL60 enroll_indicator_: elig_period_: elig_indicator_:
    	MA_indicator_: MA_elig_indicator_: FFS_indicator_: FFS_elig_indicator_:
    	dual_indicator_: dual_elig_indicator_:;
run;

* Merge SNF, IRF and HHA assessment data with hospital data;
proc sql;
	create table merge_all as
	select distinct main.*, 
		snf.ADMSNDT_SNF, snf.DSCHRGDT_SNF,
		irf.ADMSNDT_IRF, irf.DSCHRGDT_IRF, irf.PRVDR_NUM as PRVDRNUM_IRF, 
		hha.ADMSNDT_HHA,
		other.ADMSNDT_other, other.DSCHRGDT_other, other.PRVDR_NUM as PRVDRNUM_other
	from merged_wave1_inpatient as main
		LEFT JOIN wave1_snf_claims as snf
			on main.BENE_ID = snf.BENE_ID
		LEFT JOIN wave1_irf_pai as irf 
			on main.BENE_ID = irf.BENE_ID
		LEFT JOIN wave1_hha_assess as hha 
			on main.BENE_ID = hha.BENE_ID
		LEFT JOIN wave1_other_claims as other
			on main.BENE_ID = other.BENE_ID;
quit; 

* Patients should be sent to post-acute care instutitions within 3 days after the hospital discharge date; 
data merge_all_2;
	set merge_all;
	if 0 le ADMSNDT_SNF - NCH_BENE_DSCHRG_DT le 3 then gap_snf = ADMSNDT_SNF - NCH_BENE_DSCHRG_DT; else gap_snf = .;
	if 0 le ADMSNDT_IRF - NCH_BENE_DSCHRG_DT le 3 then gap_irf = ADMSNDT_IRF - NCH_BENE_DSCHRG_DT; else gap_irf = .;
	if 0 le ADMSNDT_HHA - NCH_BENE_DSCHRG_DT le 3 then gap_hha = ADMSNDT_HHA - NCH_BENE_DSCHRG_DT; else gap_hha = .;
	if 0 le ADMSNDT_other - NCH_BENE_DSCHRG_DT le 3 then gap_other = ADMSNDT_other - NCH_BENE_DSCHRG_DT; else gap_other=.;

	if gap_snf ne . | gap_irf ne . | gap_hha ne . | gap_other ne . then 
	do;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_other then do; disch_pac_n=4; gap=gap_other; end;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_hha then do; disch_pac_n=3; gap=gap_hha; end;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_irf then do; disch_pac_n=2; gap=gap_irf; end;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_snf then do; disch_pac_n=1; gap=gap_snf; end;
	end;
	
	if gap eq . then gap = 99;
	
	* add indicator for SP's receipt of home health at any time during 90 days;
	if 0 le ADMSNDT_HHA - NCH_BENE_DSCHRG_DT le 90 then receipt_hha_90day = 1;
		else receipt_hha_90day = 0;
run; 

* collapse down to bene/discharge date level;
proc sql;
	create table hha_indicator as 
	select BENE_ID, NCH_BENE_DSCHRG_DT, max(receipt_hha_90day) as receipt_hha_90day
	from merge_all_2
	group by BENE_ID, NCH_BENE_DSCHRG_DT;
	quit;

proc sort data = merge_all_2;
	by BENE_ID NCH_BENE_DSCHRG_DT gap; 
run;

data merge_all_3;
	set merge_all_2;
	by BENE_ID NCH_BENE_DSCHRG_DT gap;
	if first.NCH_BENE_DSCHRG_DT then output;
run;

proc sql;
	create table merge_all_4 as
	select a.*, b.receipt_hha_90day
	from merge_all_3 as a join hha_indicator as b
		on a.BENE_ID = b.BENE_ID and a.NCH_BENE_DSCHRG_DT = b.NCH_BENE_DSCHRG_DT;
quit;

data wave1_merge_all_updated;
	set merge_all_4;
	if PTNT_DSCHRG_STUS_CD in ('41','42','50','51') then hospice = 1;
		else if PTNT_DSCHRG_STUS_CD not in ('41','42','50','51') and PTNT_DSCHRG_STUS_CD ne . then hospice = 0;
	if disch_pac_n = . and hospice = 1 then disch_pac_n = 5;
		else if disch_pac_n = . then disch_pac_n = 0;
	format disch_pac_n pacf_n.;
run; 

/*******************************************************************************
*** WAVE 2
*******************************************************************************/
* import NHATS-CMS inpatient claims;
filename ipt2014b "/PATH/inpatient_base_claims_j_read_v8_2014_wave2.sas";
%include ipt2014b;

filename ipt2015b "/PATH/inpatient_base_claims_j_read_v8_2015.sas";
%include ipt2015b;

filename ipt2016b "/PATH/inpatient_base_claims_k_read_v8_2016.sas";
%include ipt2016b;

filename ipt2017b "/PATH/inpatient_base_claims_k_read_v8_2017.sas";
%include ipt2017b;


* combine all inpatient claims for Wave 2;
data wave2_inpatient;
	length CLM_ADMSN_DT 8.;
	set inpatient_base_claims_j_2014b
		inpatient_base_claims_j_2015
		inpatient_base_claims_k_2016
		inpatient_base_claims_k_2017;
	
	* extract discharge year and month;
	dschrg_year = year(NCH_BENE_DSCHRG_DT);
	dschrg_month = month(NCH_BENE_DSCHRG_DT);
	los_inpatient = NCH_BENE_DSCHRG_DT - CLM_ADMSN_DT;
	keep BENE_ID PRVDR_NUM NCH_CLM_TYPE_CD NCH_PTNT_STATUS_IND_CD PTNT_DSCHRG_STUS_CD
		CLM_ADMSN_DT NCH_BENE_DSCHRG_DT dschrg_year dschrg_month los_inpatient CLM_DRG_CD;
run; * 9871 obs;

* filter inpatient hospitalization;
data wave2_inp_hosp;
	set wave2_inpatient;
	if (substr(PRVDR_NUM,3,1) = '0' or substr(PRVDR_NUM,3,2) = '13') /* restrict to claims from GAC and CAH */
		and NCH_CLM_TYPE_CD in ("60", "61", "62", "63", "64")		/* restrict to inpatient claim type 60-64; */
		and NCH_PTNT_STATUS_IND_CD = "A";		/* restrict to discharge status = discharged alive (as opposed to died or still a patient)*/
run; * 8905 obs;

* Get unique record for each BENE_ID on same discharge date;
proc sort data = wave2_inp_hosp;
	by BENE_ID NCH_BENE_DSCHRG_DT;
run;

data wave2_inp_hosp;
	set wave2_inp_hosp;
	by BENE_ID NCH_BENE_DSCHRG_DT;
	if first.NCH_BENE_DSCHRG_DT then output;
run; * 8904 obs;

* merge with MBSF data;
proc sql;
	create table merged_wave2 as
	select *
	from (
		select *
		from wave2_inp_hosp as a
			left join wave2_mbsf as b
			on a.BENE_ID = b.BENE_ID) as c
		left join wave2_mbsf_d (keep = BENE_ID DUAL:) as d
		on c.BENE_ID = d.BENE_ID;			
quit;

* determine part A, part B, and MA status from admission to post discharge;
* add dual eligibility;
data merged_wave2_inpatient;
	set merged_wave2;
	array buyin{48} BUYIN1 - BUYIN48;
	array hmoin{48} HMOIND1 - HMOIND48;
	array elig_period{48} elig_period_1 - elig_period_48;

	array enroll_indicator{48} enroll_indicator_1 - enroll_indicator_48;
	array elig_indicator{48} elig_indicator_1 - elig_indicator_48;
	array MA_indicator(48) MA_indicator_1 - MA_indicator_48;
	array MA_elig_indicator(48) MA_elig_indicator_1 - MA_elig_indicator_48;
	array FFS_indicator(48) FFS_indicator_1 - FFS_indicator_48;
	array FFS_elig_indicator(48) FFS_elig_indicator_1 - FFS_elig_indicator_48;
	
	array dual{48} DUAL1 - DUAL48;
	array dual_indicator{48} dual_indicator_1 - dual_indicator_48;
	array dual_elig_indicator{48} dual_elig_indicator_1 - dual_elig_indicator_48;
	
	if DEATH_DT ne . & DEATH_DT ge NCH_BENE_DSCHRG_DT 
		then post_dischdt = min(NCH_BENE_DSCHRG_DT + 90 , DEATH_DT); 
		else post_dischdt = NCH_BENE_DSCHRG_DT + 90;
	
	do i = 1 to 48;
		
	* eligible period;
	if (year(CLM_ADMSN_DT) - 2014) * 12 + month(CLM_ADMSN_DT) le i le (year(post_dischdt) - 2014) * 12 + month(post_dischdt) 
		then elig_period(i) = 1; 
    	else elig_period(i) = 0; 
    	
	* Eligible for Part A & B coverage;
	if buyin(i) in ('3','C') 
		then enroll_indicator(i) = 1; 
		else enroll_indicator(i) = 0;
		    	
    elig_indicator(i) = enroll_indicator(i) * elig_period(i);
    
	*Eligible for Part A & B - enrolled in MA;
	if buyin(i) in ('3','C') & hmoin(i) not in (' ','0','4') 
		then MA_indicator(i) = 1; 
		else MA_indicator(i) = 0;
		
	MA_elig_indicator(i) = MA_indicator(i) * elig_period(i);
	
	*Eligible for Part A & B - enrolled in FFS;
	if buyin(i) in ('3','C') & hmoin(i) in (' ','0','4') 
		then FFS_indicator(i) = 1; 
		else FFS_indicator(i) = 0;
		
	FFS_elig_indicator(i) = FFS_indicator(i) * elig_period(i);
	
	* dual eligible;
	if dual(i) in ('02', '04', '08')
		then dual_indicator(i) = 1;
		else dual_indicator(i) = 0;
	
	dual_elig_indicator(i) = dual_indicator(i) * elig_period(i);
	end; 

	drop i;
	elig_sum = sum(of elig_indicator_1-elig_indicator_48);
	MA_elig_sum = sum(of MA_elig_indicator_1-MA_elig_indicator_48);
	FFS_elig_sum = sum(of FFS_elig_indicator_1-FFS_elig_indicator_48);
	dual_elig_sum = sum(of dual_elig_indicator_1-dual_elig_indicator_60);
	
	*Continuously eligible for Part A & B;
	if elig_sum eq ((year(post_dischdt) - year(CLM_ADMSN_DT)) * 12 + month(post_dischdt) - month(CLM_ADMSN_DT) + 1)
		then enrollment_elig = 1; 
		else enrollment_elig = 0;

	*Continuously eligible for Part A & B - continuously enrolled in MA;
	if MA_elig_sum eq ((year(post_dischdt) - year(CLM_ADMSN_DT)) * 12 + month(post_dischdt) - month(CLM_ADMSN_DT) + 1)
		then MA_enrollment_elig = 1; 
		else MA_enrollment_elig = 0;

	*Continuously eligible for Part A & B - continuously enrolled in FFS;
	if FFS_elig_sum eq ((year(post_dischdt) - year(CLM_ADMSN_DT)) * 12 + month(post_dischdt) - month(CLM_ADMSN_DT) + 1)
		then FFS_enrollment_elig = 1;
		else FFS_enrollment_elig = 0;
		
	*Ever dual eligible;
	if dual_elig_sum > 0
		then dual_eligible = 1;
		else dual_eligible = 0;
		
	label MA_enrollment_elig="Continuous Enrollment in Medicare Advantage" 
    	  FFS_enrollment_elig="Continuous Enrollment in Fee-for-Service";
    	  
    drop BUYIN: HMOIND: DUAL1 - DUAL48 enroll_indicator_: elig_period_: elig_indicator_:
    	MA_indicator_: MA_elig_indicator_: FFS_indicator_: FFS_elig_indicator_:
    	dual_indicator_: dual_elig_indicator_:;
run; 

* Merge SNF, IRF and HHA data with hospital data;
proc sql;
	create table merge_all as
	select distinct main.*, 
		snf.ADMSNDT_SNF, snf.DSCHRGDT_SNF,
		irf.ADMSNDT_IRF, irf.DSCHRGDT_IRF, irf.PRVDR_NUM as PRVDRNUM_IRF, 
		hha.ADMSNDT_HHA,
		other.ADMSNDT_other, other.DSCHRGDT_other, other.PRVDR_NUM as PRVDRNUM_other
	from merged_wave2_inpatient as main
		LEFT JOIN wave2_snf_claims as snf  /*******note here we switched to SNF claims******/
			on main.BENE_ID = snf.BENE_ID
		LEFT JOIN wave2_irf_pai as irf 
			on main.BENE_ID = irf.BENE_ID
		LEFT JOIN wave2_hha_assess as hha 
			on main.BENE_ID = hha.BENE_ID
		LEFT JOIN wave2_other_claims as other
			on main.BENE_ID = other.BENE_ID;
quit; 

* Patients should be sent to post-acute care instutitions within 3 days 
* after the hospital discharge date; 
data merge_all_2;
	set merge_all;
	if 0 le ADMSNDT_SNF - NCH_BENE_DSCHRG_DT le 3 then gap_snf = ADMSNDT_SNF - NCH_BENE_DSCHRG_DT; else gap_snf = .;
	if 0 le ADMSNDT_IRF - NCH_BENE_DSCHRG_DT le 3 then gap_irf = ADMSNDT_IRF - NCH_BENE_DSCHRG_DT; else gap_irf = .;
	if 0 le ADMSNDT_HHA - NCH_BENE_DSCHRG_DT le 3 then gap_hha = ADMSNDT_HHA - NCH_BENE_DSCHRG_DT; else gap_hha = .;
	if 0 le ADMSNDT_other - NCH_BENE_DSCHRG_DT le 3 then gap_other = ADMSNDT_other - NCH_BENE_DSCHRG_DT; else gap_other=.;

	if gap_snf ne . | gap_irf ne . | gap_hha ne . | gap_other ne . then 
	do;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_other then do; disch_pac_n=4; gap=gap_other; end;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_hha then do; disch_pac_n=3; gap=gap_hha; end;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_irf then do; disch_pac_n=2; gap=gap_irf; end;
		if min(gap_snf, gap_irf, gap_hha, gap_other) eq gap_snf then do; disch_pac_n=1; gap=gap_snf; end;
	end;
	
	if gap eq . then gap = 99;
	
	* add indicator for SP's receipt of home health at any time during 90 days;
	if 0 le ADMSNDT_HHA - NCH_BENE_DSCHRG_DT le 90 then receipt_hha_90day = 1;
		else receipt_hha_90day = 0;
run; 

* collapse down to bene/discharge date level;
proc sql;
	create table hha_indicator as 
	select BENE_ID, NCH_BENE_DSCHRG_DT, max(receipt_hha_90day) as receipt_hha_90day
	from merge_all_2
	group by BENE_ID, NCH_BENE_DSCHRG_DT;
	
	select count(*) from hha_indicator; * 8904 obs;
quit;


proc sort data = merge_all_2;
	by BENE_ID NCH_BENE_DSCHRG_DT gap; 
run;

data merge_all_3;
	set merge_all_2;
	by BENE_ID NCH_BENE_DSCHRG_DT gap;
	if first.NCH_BENE_DSCHRG_DT then output;
run;

proc sql;
	create table merge_all_4 as
	select a.*, b.receipt_hha_90day
	from merge_all_3 as a join hha_indicator as b
		on a.BENE_ID = b.BENE_ID and a.NCH_BENE_DSCHRG_DT = b.NCH_BENE_DSCHRG_DT;

	select count(*) from merge_all_4; * 8904 obs;
quit;

data wave2_merge_all_updated;
	set merge_all_4;
	if PTNT_DSCHRG_STUS_CD in ('41','42','50','51') then hospice = 1;
		else if PTNT_DSCHRG_STUS_CD not in ('41','42','50','51') and PTNT_DSCHRG_STUS_CD ne . then hospice = 0;
	if disch_pac_n = . and hospice = 1 then disch_pac_n = 5;
		else if disch_pac_n = . then disch_pac_n = 0;
	format disch_pac_n pacf_n.;
run; 


