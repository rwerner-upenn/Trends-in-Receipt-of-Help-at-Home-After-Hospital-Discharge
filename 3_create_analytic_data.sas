/*******************************************************************************
** Goal: to create the analytic cohort, including all SP goint to home/home health agency
*******************************************************************************/

/*******************************************************************************
*** Step 1: load NHATS main survey data, restrict to community-dwelling and age 69+
*******************************************************************************/
%macro round(num, wave, year);

proc sql;
	* check number of SP;
	select count(distinct spid) as n_SP
	from r&num._NHATS_analytic;
	
	* check distribution of dresid;
	select dresid, count(distinct spid)
	from r&num._NHATS_analytic
	group by dresid;
	
	* check #spid age 69+;
	select count(distinct spid) as nspid_69
	from r&num._NHATS_analytic
	where age >= 69;

	create table r&num._nhats as
	select *
	from r&num._NHATS_analytic
	where dresid in (1, 6) and age >= 69;
	
quit;


/*******************************************************************************
*** Step 2: merge with CMS claims data
*******************************************************************************/
proc sql;
	create table r&num._claims_updated as
		/* living SP, or deceased SP with missing death date */
	select *
	from r&num._nhats as a join wave&wave._merge_all_updated (drop = DEATH_DT) as b
		on a.bene_id = b.bene_id
			and 
			(a.dresid ^= 6 OR DEATH_DT = .)
			and
			mdy(a.r&num.casestdtmt, 1, a.sp_year) - 365 <= b.NCH_BENE_DSCHRG_DT <= mdy(a.r&num.casestdtmt, 1, a.sp_year)
	UNION ALL
		/* deceased SP with valid death date*/
		(select *
		from r&num._nhats as c join wave&wave._merge_all_updated (drop = DEATH_DT) as d
		on c.bene_id = d.bene_id
			and 
			(c.dresid = 6 AND DEATH_DT > 0)
			and
			DEATH_DT - 365 <= d.NCH_BENE_DSCHRG_DT <= DEATH_DT
		)
	ORDER BY SPID, NCH_BENE_DSCHRG_DT;
quit;


/*******************************************************************************
*** Step 3: RANDOMLY SELECT ONE HOSPITALIZATIONS PER SPID
*******************************************************************************/
proc surveyselect data = r&num._claims_updated
		out = r&num._claims_single
		method = srs
		sampsize = 1
		seed = 4007;
	strata spid;
run;

* check;
proc sql;
	select count(distinct spid), count(distinct bene_id), count(*) 
	from r&num._claims_single;
quit;	

%mend round;
%round(1, 1, 2011)
%round(2, 1, 2012)
%round(3, 1, 2013)
%round(4, 1, 2014)
%round(5, 2, 2015)
%round(6, 2, 2016)
%round(7, 2, 2017)

/*******************************************************************************
*** Step 4: RESTRICT TO HOME + HOME HEALTH AGENCY
*******************************************************************************/
%macro round(num, wave, year);

proc freq data = r&num._claims_single;
	tables disch_pac_n;
run;

data r&num._claims_single_home_hha;
	set r&num._claims_single;
	if disch_pac_n in (0, 3);
run;

* check;
proc sql;
	select count(distinct spid), count(distinct bene_id), count(*) 
	from r&num._claims_single_home_hha;
quit;	


%mend round;
%round(1, 1, 2011)
%round(2, 1, 2012)
%round(3, 1, 2013)
%round(4, 1, 2014)
%round(5, 2, 2015)
%round(6, 2, 2016)
%round(7, 2, 2017)


/*******************************************************************************
*** Step 5: create outcome variables at discharge/SP level
*******************************************************************************/
* type of outcome, at discharge/SP level

1 - 		help received during post-acute period (valid duration)
0 - 		no help at all (missing duration with indicator for no help)
			help but not during post-acute period (valid duration)
missing -	help but unable to determine duration of help (invalid duration with an indicator for unknow duration)
			entire section missing, FQ questionaire (missing duration)
			entire section ineligible, Nursing Home (missing duration)
			entire section skipped, Other reason (missing duration);

%macro outcome(num);

/******* discharge level*******/
data r&num._analytic_prep1;				
	
	set r&num._claims_single_home_hha;
	
	/* compare help duration dates with post-acute period */
	* note here the data is at discharge level, not person level;

	/* MO */
	if mo_start_date = . then 
	do;
		/* missing dates b/c no help at all */
		if mo_ever_help = 0 then mo_help = 0;
		
		/* missing dates b/c unable to determine duration */
		else if mo_unknown_duration = 1 then mo_help = .;
		
		/* missing dates b/c no data (FQ/NH/other) */
		else mo_help = .;
	end;
	
	else if mo_start_date > 0 then
	do;
		/* valid dates, NO overlapping with post-acute period */
		if mo_end_date < NCH_BENE_DSCHRG_DT 
			OR mo_start_date > NCH_BENE_DSCHRG_DT + 90 then mo_help = 0;
	
		/* valid dates, IS overlapping with post-acute period */
		else 
		do;
			mo_help = 1;
			* calculate the portion of cg duration after discharge (within round);
			if mo_start_date <= NCH_BENE_DSCHRG_DT
				then mo_cg_length = mo_end_date - NCH_BENE_DSCHRG_DT;
			else if mo_start_date > NCH_BENE_DSCHRG_DT
				then mo_cg_length = mo_end_date - mo_start_date;
				
			if mo_cg_length > 30 then mo_cg_length30 = 1;
				else mo_cg_length30 = 0;
			if mo_cg_length > 60 then mo_cg_length60 = 1;
				else mo_cg_length60 = 0;
			if mo_cg_length > 90 then mo_cg_length90 = 1;
				else mo_cg_length90 = 0;
		end;
	end;	
	
	/* SC */
	if sc_start_date = . then 
	do;
		/* missing dates b/c no help at all */
		if sc_ever_help = 0 then sc_help = 0;
		
		/* missing dates b/c unable to determine duration */
		else if sc_unknown_duration = 1 then sc_help = .;
		
		/* missing dates b/c no data (FQ/NH/other) */
		else sc_help = .;
	end;
	
	else if sc_start_date > 0 then
	do;	
		/* valid dates, NO overlapping with post-acute period */
		if sc_end_date < NCH_BENE_DSCHRG_DT 
			OR sc_start_date > NCH_BENE_DSCHRG_DT + 90 then sc_help = 0;
	
		/* valid dates, IS overlapping with post-acute period */
		else 
		do;
			sc_help = 1;
			* calculate the portion of cg duration after discharge (within round);
			if sc_start_date <= NCH_BENE_DSCHRG_DT
				then sc_cg_length = sc_end_date - NCH_BENE_DSCHRG_DT;
			else if sc_start_date > NCH_BENE_DSCHRG_DT
				then sc_cg_length = sc_end_date - sc_start_date;
			
			if sc_cg_length > 30 then sc_cg_length30 = 1;
				else sc_cg_length30 = 0;
			if sc_cg_length > 60 then sc_cg_length60 = 1;
				else sc_cg_length60 = 0;
			if sc_cg_length > 90 then sc_cg_length90 = 1;
				else sc_cg_length90 = 0;
		end;
	end;	
run;

%mend outcome;
%outcome(1)
%outcome(2)
%outcome(3)
%outcome(4)
%outcome(5)
%outcome(6)
%outcome(7)


/*******************************************************************************
*** Step 6: did the SP receive help at different months after discharge
*******************************************************************************/

%macro timepoint(num);

data r&num._analytic_prep2;
	set r&num._analytic_prep1;

	dest = disch_pac_n;

	* extract month of discharge;
	discharge_month = (year(NCH_BENE_DSCHRG_DT) - 2010) * 12 + month(NCH_BENE_DSCHRG_DT);

	* for each month before/after discharge, compare with caregiving monthly lookup variables;
	array mo_help_month {108} mo_help_month_1 - mo_help_month_108;
	array sc_help_month {108} sc_help_month_1 - sc_help_month_108;

	array mo(11) mo_cg_n1 mo_cg_0 mo_cg_1 mo_cg_2 mo_cg_3 mo_cg_4 mo_cg_5 mo_cg_6 mo_cg_7 mo_cg_8 mo_cg_9;
	array sc(11) sc_cg_n1 sc_cg_0 sc_cg_1 sc_cg_2 sc_cg_3 sc_cg_4 sc_cg_5 sc_cg_6 sc_cg_7 sc_cg_8 sc_cg_9;
	array cg(11) cg_n1 cg_0 cg_1 cg_2 cg_3 cg_4 cg_5 cg_6 cg_7 cg_8 cg_9;

	do i = 1 to 11;
		mo(i) =  mo_help_month(discharge_month + i - 2);
		sc(i) =  sc_help_month(discharge_month + i - 2);

		if mo(i) = 1 or sc(i) = 1 then cg(i) = 1;
			else if mo(i) = . and sc(i) = . then cg(i) = .;
			else cg(i) = 0;
	end;

	/* composite */
	adl_prior_disch = cg_n1;

run;

%mend timepoint;
%timepoint(1)
%timepoint(2)
%timepoint(3)
%timepoint(4)
%timepoint(5)
%timepoint(6)
%timepoint(7)


/*******************************************************************************
*** Step 7: create outcome variables at SP level 
*******************************************************************************/
%macro finaloutcome(num);

proc sql;
	select count(distinct spid) as n_spid, count(*) as n
	from r&num._analytic_prep2;
quit;

proc sql;
	create table r&num._outcome_only as
	select spid, 
		dest,
		(case when dest = 0 then 0
		when dest in (1, 2, 4, 5) then 1 
		when dest = 3 then 2 
		else . end) as destination,
		(case when dest = 5 then 4
		else dest end) as destination2,
		receipt_hha_90day,

		mo_help as help_MO,
		sc_help as help_SC,
		(case when help_MO = 1 or help_SC = 1 then 1 else 0 end) as help_ADL,
		
		(case when calculated help_ADL = 1 and receipt_hha_90day = 0 then 1 
		else 0 end) as help_ADL_no_HHA, 

		dual_eligible as dual_eligible,

		mo_cg_n1, mo_cg_0, mo_cg_1, mo_cg_2, mo_cg_3, mo_cg_4, mo_cg_5, mo_cg_6, mo_cg_7, mo_cg_8, mo_cg_9, 
		sc_cg_n1, sc_cg_0, sc_cg_1, sc_cg_2, sc_cg_3, sc_cg_4, sc_cg_5, sc_cg_6, sc_cg_7, sc_cg_8, sc_cg_9, 
		cg_n1, cg_0, cg_1, cg_2, cg_3, cg_4, cg_5, cg_6, cg_7, cg_8, cg_9, adl_prior_disch
		
		from r&num._analytic_prep2
quit;

%mend finaloutcome;
%finaloutcome(1)
%finaloutcome(2)
%finaloutcome(3)
%finaloutcome(4)
%finaloutcome(5)
%finaloutcome(6)
%finaloutcome(7)


%macro finaldemo(num);

proc sql;
	create table sp_demo_&num. as
	select distinct spid, gender, race, race2, marital, SP_YEAR, BENE_BIRTH_DT, DEATH_DT,
		age, agecat_mbsf, agecat_final,
		overall_health,
		needhelp_mo,
		needhelp_sc,
		needhelp_ha,
		needhelp_mc,
		demclas,
		baseline_demclas, 
		(case when baseline_demclas in (1, 2) then 1
			when baseline_demclas = 3 then 0
			when baseline_demclas = -1 then -1
			when baseline_demclas = -9 then -1
			else . end) as baseline_demclas_new,
		baseline_overall_health, 
		baseline_independent_mc, 
		baseline_independent_ha, 
		baseline_independent_sc, 
		baseline_independent_mo,
		los_inpatient,
		CLM_DRG_CD as drg_code,
		(SP_YEAR - 2010) as round, 
		r&num.status as status,
		ana_final_wt0, 
		ana_2011_wt0,
		varstrata,
		varunit, 
		R&num.CASESTDTMT as CASESTDTMT,
		R&num.CASESTDTYR as CASESTDTYR, 
		YEARSAMPLE, 
		n_helper, sum_hrs_month, diff_hrs_month,  diff_1st_2nd/*NSOC*/
	from r&num._analytic_prep2;
	
%mend finaldemo;
%finaldemo(1)
%finaldemo(5)
%finaldemo(7)

%macro finaldemo(num);

proc sql;
	create table sp_demo_&num. as
	select distinct spid, gender, race, race2, marital, SP_YEAR, BENE_BIRTH_DT, DEATH_DT,
		age, agecat_mbsf, agecat_final,
		overall_health,
		needhelp_mo,
		needhelp_sc,
		needhelp_ha,
		needhelp_mc,
		demclas,
		baseline_demclas, 
		(case when baseline_demclas in (1, 2) then 1
			when baseline_demclas = 3 then 0
			when baseline_demclas = -1 then -1
			when baseline_demclas = -9 then -1
			else . end) as baseline_demclas_new,
		baseline_overall_health, 
		baseline_independent_mc, 
		baseline_independent_ha, 
		baseline_independent_sc, 
		baseline_independent_mo,
		los_inpatient,
		CLM_DRG_CD as drg_code,
		(SP_YEAR - 2010) as round, 
		r&num.status as status,
		ana_final_wt0, 
		ana_2011_wt0,
		varstrata,
		varunit, 
		R&num.CASESTDTMT as CASESTDTMT,
		R&num.CASESTDTYR as CASESTDTYR, 
		YEARSAMPLE, 
		1 as n_helper, 1 as sum_hrs_month, 1 as diff_hrs_month, 1 as diff_1st_2nd /*NSOC placeholder*/
	from r&num._analytic_prep2;

quit;

%mend finaldemo;
%finaldemo(2)
%finaldemo(3)
%finaldemo(4)
%finaldemo(6)


%macro final(num);

proc sql;
	create table r&num._analytic as
	select a.*, b.*
	from sp_demo_&num. as a 
		join r&num._outcome_only as b on a.spid = b.spid;
quit;

* remove NA to both outcome variable;
data r&num._analytic;
	set r&num._analytic;
	if help_MO = . and help_SC = . then delete;
run;

%mend final;
%final(1)
%final(2)
%final(3)
%final(4)
%final(5)
%final(6)
%final(7)


data pac_home_hha_analytic_0518;
	set r1_analytic
		r2_analytic
		r3_analytic
		r4_analytic
		r5_analytic
		r6_analytic
		r7_analytic;
run;

* output top 10 DRG code;
%let TopN = 10;
proc freq data = pac_home_hha_analytic_0518 ORDER=FREQ;
	weight ANA_FINAL_WT0;
	tables DRG_CODE / maxlevels = &TopN;
run;

* recode top DRG groupS to binary;
data pac_home_hha_analytic_0518;
	set pac_home_hha_analytic_0518;
	if DRG_CODE = 470 then DRG470 = 1; else DRG470 = 0;
	if DRG_CODE = 871 then DRG871 = 1; else DRG871 = 0;
	if DRG_CODE = 392 then DRG392 = 1; else DRG392 = 0;
	if DRG_CODE = 194 then DRG194 = 1; else DRG194 = 0;
	if DRG_CODE = 603 then DRG603 = 1; else DRG603 = 0;
	if DRG_CODE = 292 then DRG292 = 1; else DRG292 = 0;

proc export data = pac_home_hha_analytic_0518
			dbms = csv
			outfile = "/PATH/nhats_analytic_single_home_hha_20210518.csv"
			replace;
run;




