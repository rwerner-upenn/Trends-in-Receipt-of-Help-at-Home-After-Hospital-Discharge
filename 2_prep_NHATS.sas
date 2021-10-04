/*******************************************************************************
** Goal: prepare NHATS R1-R7 data before merging in hospitalization data
*******************************************************************************/


* PRE-PROCESSING: PREP DATA SO VARIABLES ARE CONSISTENT ACROSS ROUNDS;
/*******************************************************************************
*** Step 0a: prep NHATS - extract race and gender for Round 1 & Round 5 
*** this is because some rounds are missing these information
*******************************************************************************/
proc sql;
	create table wave1_gender_race as
	select spid, R1DGENDER as gender, 
		(case when RL1DRACEHISP = 6 then 5
			else RL1DRACEHISP end) as race
	from r1pub.spfile;
	
	create table wave2_gender_race as
	select spid, R5DGENDER as gender, 
		(case when RL5DRACEHISP = 6 then 5
			else RL5DRACEHISP end) as race
	from r5pub.spfile;
quit;

/*******************************************************************************
*** Step 0b: prep NHATS - extract marital status by round
*** this is because in follow up rounds only changes were reported so linking between rounds is required
*******************************************************************************/
proc sql;
	create table round1_marital as
	select spid, hh1martlstat as marital
	from r1pub.spfile;
	
	/* starting in R2, linking to previous round*/
	create table round2_marital as
	select a.spid, 
		(case when hh2marchange = 1 or hh2martlstat > 0 then hh2martlstat 
		else b.marital end) as marital
	from r2pub.spfile as a left join round1_marital as b
		on a.spid = b.spid;
		
	create table round3_marital as
	select a.spid, 
		(case when hh3marchange = 1 or hh3martlstat > 0 then hh3martlstat 
		else b.marital end) as marital
	from r3pub.spfile as a left join round2_marital as b
		on a.spid = b.spid;
		
	create table round4_marital as
	select a.spid, 
		(case when hh4marchange = 1 or hh4martlstat > 0 then hh4martlstat 
		else b.marital end) as marital
	from r4pub.spfile as a left join round3_marital as b
		on a.spid = b.spid;
		
	create table round5_marital as
	select a.spid, 
		(case when r5dcontnew = 2 then hh5martlstat
		when r5dcontnew = 1 and (hh5marchange = 1 or hh5martlstat > 0) then hh5martlstat 
		else b.marital end) as marital
	from r5pub.spfile as a left join round4_marital as b
		on a.spid = b.spid;
	
	create table round6_marital as
	select a.spid, 
		(case when hh6marchange = 1 or hh6martlstat > 0 then hh6martlstat 
		else b.marital end) as marital
	from r6pub.spfile as a left join round5_marital as b
		on a.spid = b.spid;

	create table round7_marital as
	select a.spid, 
		(case when hh7marchange = 1 or hh7martlstat > 0 then hh7martlstat 
		else b.marital end) as marital
	from r7pub.spfile as a left join round6_marital as b
		on a.spid = b.spid;
quit;

* collapose marital categories;
%macro round(num, wave, year);
   data round&num._marital;
   	   set round&num._marital;
   	   if marital in (1, 2) then mari = 1;
   	   else if marital in (3, 4, 5) then mari = 2;
   	   else if marital = 6 then mari = 3;
   	   else mari = .;
   	   drop marital;
   	   rename mari = marital;
   run;
%mend round;
%round(1, 1, 2011)
%round(2, 1, 2012)
%round(3, 1, 2013)
%round(4, 1, 2014)
%round(5, 2, 2015)
%round(6, 2, 2016)
%round(7, 2, 2017)


/*******************************************************************************
*** Step 0c: fix R2 help starting/ending months
*******************************************************************************/
* note 2012 code this variable differently;
data  r2pub.spfile;
	set r2pub.spfile;
	if dm2helpstyr > 0 then dm2helpstyr = dm2helpstyr + 2010;
	if dm2helpendyr > 0 then dm2helpendyr = dm2helpendyr + 2010;
	if ds2helpstyr > 0 then ds2helpstyr = ds2helpstyr + 2010;
	if ds2helpendyr > 0 then ds2helpendyr = ds2helpendyr + 2010;
run;


/*******************************************************************************
*** Step 1: load NHATS main survey data
*** link with SP tracker file to get interview year/month information
*** link with SP demographic file to get sample person demographics (age, race, marital)
*** note R5-R7 have a different 2011 cohort weight
*******************************************************************************/
%macro round(num, wave, year);

data _null_;
	file print;
     put _page_;
     put "*******This is Round &num. *******";
run;

%if &num. < 5 %then %do;
proc sql;
	create table r&num._sp_combined as
	select &year. as sp_year,
		 a.*,
		 a.r&num.d2intvrage as liveagecat, a.r&num.d2deathage as deathagecat, 
		 (case when liveagecat > 0 then liveagecat
		 when liveagecat < 0 and deathagecat > 0 then deathagecat
		 else . end) as agecat_nhats,
		 (case when a.hc&num.health in (-9, -8, -7) then .
			else a.hc&num.health end) as overall_health,
		 a.W&num.ANFINWGT0 as ana_final_wt0, 		
		 a.W&num.ANFINWGT0 as ana_2011_wt0,
		 b.*, 
		 c.pd&num.mthdied as mthdied,
		 c.pd&num.yrdied as yrdied,
		 d.gender, d.race, e.marital
	from (
		select * 
		from r&num.pub.spfile 
			(keep = spid r&num.dresid
			 r&num.d2intvrage r&num.d2deathage
			 is&num.: hc&num.: sc&num.: mo&num.: dm&num.: ds&num.:
			 ha&num.: mc&num.:
			 W&num.ANFINWGT0 W&num.VARSTRAT W&num.VARUNIT)
		) as a,
		r&num.pub.tracker_file (keep = SPID YEARSAMPLE R:) as b,
		r&num.dem.sp_demo as c,
		wave&wave._gender_race as d, 
		round&num._marital as e
	where a.spid = b.spid = c.spid = d.spid = e.spid;
quit;
%end;

%else %if &num. >= 5 %then %do;
proc sql;
	create table r&num._sp_combined as
	select &year. as sp_year,
		 a.*,
		 a.r&num.d2intvrage as liveagecat, a.r&num.d2deathage as deathagecat, 
		  (case when liveagecat > 0 then liveagecat
		 when liveagecat < 0 and deathagecat > 0 then deathagecat
		 else . end) as agecat_nhats,
		 (case when a.hc&num.health in (-9, -8, -7) then .
			else a.hc&num.health end) as overall_health,
		 a.W&num.ANFINWGT0 as ana_final_wt0, 	
		 a.W&num.AN2011WGT0 as ana_2011_wt0,
		 b.*, 
		 c.pd&num.mthdied as mthdied,
   		 c.pd&num.yrdied as yrdied,
		 d.gender, d.race, e.marital
	from (
		select * 
		from r&num.pub.spfile 
			(keep = spid r&num.dresid
			 r&num.d2intvrage r&num.d2deathage
			 is&num.: hc&num.: sc&num.: mo&num.: dm&num.: ds&num.:
			 ha&num.: mc&num.:
			 W&num.ANFINWGT0 W&num.AN2011WGT0 W&num.VARSTRAT W&num.VARUNIT)
		) as a,
		r&num.pub.tracker_file (keep = SPID YEARSAMPLE R:) as b,
		r&num.dem.sp_demo as c,
		wave&wave._gender_race as d, 
		round&num._marital as e
	where a.spid = b.spid = c.spid = d.spid = e.spid;
quit;
%end;


/*******************************************************************************
*** Step 2: link with xwalk to get CMS BENE ID added to NHATS data
*******************************************************************************/
proc sql;
	create table r&num._sp_tracker_beneid as
	select a.*, b.bene_id
	from r&num._sp_combined as a join nhats_xwalk_combined as b
		on a.spid = b.spid;

	create table r&num._sp_tracker_beneid_ffs as
	select a.*, b.ffs
	from r&num._sp_tracker_beneid as a
		left join ffs_bene_id as b
		on a.bene_id = b.bene_id;

quit;


/*******************************************************************************
*** Step 3: merge with CMS MBSF data
*******************************************************************************/
* link by bene_id;
proc sql;
	create table r&num._NHATS_MBSF as
	select a.*, b.BENE_BIRTH_DT, b.DEATH_DT
	from r&num._sp_tracker_beneid_ffs as a join wave&wave._mbsf as b
		on a.bene_id = b.bene_id;
quit;

/*******************************************************************************
*** Step 4: calculate age at interview or age at death
*******************************************************************************/
* age at interview (compare birth date with first day of the interview month);
* age at death (compare birth date with death date, for dresid == 6);

data r&num._NHATS_age;
	set r&num._NHATS_MBSF;
	intv_date = mdy(r&num.casestdtmt, 1, r&num.casestdtyr);
	
	* death age;
	if r&num.status = 62 and DEATH_DT > 0 then
	age = floor((intck('month',BENE_BIRTH_DT,DEATH_DT)-(day(DEATH_DT)<day(BENE_BIRTH_DT)))/12);
	
	* live age at interview;
	else age = floor((intck('month',BENE_BIRTH_DT,intv_date)-(day(intv_date)<day(BENE_BIRTH_DT)))/12);
	drop intv_date;
	
	
	if 65 <= age <= 69 then agecat_mbsf = 1;
	else if 70 <= age <= 74 then agecat_mbsf = 2;
	else if 75 <= age <= 79 then agecat_mbsf = 3;
	else if 80 <= age <= 84 then agecat_mbsf = 4;
	else if 85 <= age <= 89 then agecat_mbsf = 5;
	else if 90 <= age then agecat_mbsf = 6;
	else agecat_mbsf = .;	
	
	if 69 <= age <= 79 then agecat_final = 1;
	else if 80 <= age <= 89 then agecat_final = 2;
	else if 90 <= age then agecat_final = 3;
	else agecat_final = .;	
run;

/*******************************************************************************
*** Step 5: code needing help with ADL
*******************************************************************************/
data r&num._needhelp;
	set r&num._NHATS_age;

	* code for each activity; 
	/**************************************************************/
	/* MO: get around outside
	/**************************************************************/
	if mo&num.outoft = 5 /* MO1: never go outside */
		or mo&num.outwout = 1 /* MO10: ever not go outside b/c no help/diff */
		or mo&num.outdif in (3, 4) /* MO8: have difficulty doing by self */
	then needhelp_mo_out = 1;
	else needhelp_mo_out = 0;
	
	/**************************************************************/
	/* MO: get around inside
	/**************************************************************/
	if (mo&num.oftgoarea = 5 or mo&num.oflvslepr = 5) /* MO11/MO12: never go inside */
		or mo&num.insdwout = 1 /* MO23: ever not go inside b/c no help/diff */
		or mo&num.insddif in (3, 4) /* MO21: have difficulty doing by self */
	then needhelp_mo_ins = 1;
	else needhelp_mo_ins = 0;
	
	/**************************************************************/
	/* MO: get out of bed
	/**************************************************************/
	if mo&num.bedwout = 1 /* MO23: ever stay in bed b/c no help or diff */
		or mo&num.beddif in (3, 4) /* MO21: have difficulty doing by self */
	then needhelp_mo_bed = 1;
	else needhelp_mo_bed = 0;
	
	
	/**************************************************************/
	/* SC: eating
	/**************************************************************/
	if sc&num.eatwout = 1 /* no eat b/c no help or diff */
		or sc&num.eatslfdif in (3, 4) /* have difficulty doing by self */
	then needhelp_sc_eat = 1;
	else needhelp_sc_eat = 0;
	
	/**************************************************************/
	/* SC: cleaning 
	/**************************************************************/
	if sc&num.bathwout = 1 /* no washing b/c no help or diff */
		or sc&num.bathdif in (3, 4) /* have difficulty doing by self */
	then needhelp_sc_clean = 1;	
	else needhelp_sc_clean = 0;
	
	/**************************************************************/
	/* SC: toileting
	/**************************************************************/
	if sc&num.toilwout = 1 /* wet/soil b/c no help or diff */
		or sc&num.toildif in (3, 4) /* have difficulty doing by self */
	then needhelp_sc_toilet = 1;
	else needhelp_sc_toilet = 0;
	
	/**************************************************************/
	/* SC: dressing
	/**************************************************************/
	if sc&num.dresoft = 5 
		or sc&num.dreswout = 1 /* ever no dress b/c no help/diff */
		or sc&num.dresdif in (3, 4) /* have difficulty doing by self */
	then needhelp_sc_dress = 1;
	else needhelp_sc_dress = 0;
	
	
	/**************************************************************/
	/* HA: laundry
	/**************************************************************/
	if ha&num.launwout = 1 /* go without laundry b/c no help/diff */
		or ha&num.laundif in (3, 4) /* have difficulty doing by self */
		or (ha&num.laun in (2, 3, 4) and ha&num.DLAUNREAS in (1, 3)) /* receive help b/c health/functioning reason */
	then needhelp_ha_laun = 1;
	else needhelp_ha_laun = 0;
	
	/**************************************************************/
	/* HA: shopping
	/**************************************************************/
	if ha&num.shopwout = 1 /* go without shopping b/c no help/diff */
		or ha&num.shopdif in (3, 4) /* have difficulty doing by self */
		or (ha&num.shop in (2, 3, 4) and ha&num.DSHOPREAS in (1, 3)) /* receive help b/c health/functioning reason */
	then needhelp_ha_shop = 1;
	else needhelp_ha_shop = 0;
	
	/**************************************************************/
	/* HA: make hot meals
	/**************************************************************/
	if ha&num.mealwout = 1 /* go without hot meal b/c no help/diff */
		or ha&num.mealdif in (3, 4) /* have difficulty doing by self */
		or (ha&num.meal in (2, 3, 4) and ha&num.DMEALREAS in (1, 3)) /* receive help b/c health/functioning reason */
	then needhelp_ha_meal = 1;
	else needhelp_ha_meal = 0;
	
	/**************************************************************/
	/* HA: handle bills and banking
	/**************************************************************/
	if ha&num.bankwout = 1 /* go without paying bills b/c no help/diff */
		or ha&num.bankdif in (3, 4) /* have difficulty doing by self */
		or (ha&num.bank in (2, 3, 4) and ha&num.DBANKREAS in (1, 3))  /* receive help b/c health/functioning reason */
	then needhelp_ha_bank = 1;
	else needhelp_ha_bank = 0;
	
	
	/**************************************************************/
	/* MC: keep track of medicine
	/**************************************************************/
	if mc&num.medsmis = 1 /* make mistakes b/c no help/diff */
		or mc&num.medsdif in (3, 4) /* have difficulty doing by self */
		or (mc&num.medstrk in (2, 3, 4) and mc&num.DMEDSREAS in (1, 3))  /* receive help b/c health/functioning reason */
	then needhelp_mc_meds = 1;
	else needhelp_mc_meds = 0;
		
	
* add aggregate needhelp variables;
	/**************************************************************/
	/* MO
	/**************************************************************/
	if needhelp_mo_out = 1 or needhelp_mo_ins = 1 or needhelp_mo_bed = 1 
		then needhelp_mo = 1;
		else needhelp_mo = 0;

	/**************************************************************/
	/* SC
	/**************************************************************/
	if needhelp_sc_eat = 1 or needhelp_sc_clean = 1 or needhelp_sc_toilet = 1 or needhelp_sc_dress = 1 
		then needhelp_sc = 1;
		else needhelp_sc = 0;
		
	/**************************************************************/
	/* HA
	/**************************************************************/
	if needhelp_ha_laun = 1 or needhelp_ha_shop = 1 or needhelp_ha_meal = 1 or needhelp_ha_bank = 1
		then needhelp_ha = 1;
		else needhelp_ha = 0;
	
	/**************************************************************/
	/* MC
	/**************************************************************/
	if needhelp_mc_meds = 1
		then needhelp_mc = 1;
		else needhelp_mc = 0;

run;
		
%mend round;
%round(1, 1, 2011)
%round(2, 1, 2012)
%round(3, 1, 2013)
%round(4, 1, 2014)
%round(5, 2, 2015)
%round(6, 2, 2016)
%round(7, 2, 2017)



/*******************************************************************************
*** Step 6: code duration of MO/SC help
*******************************************************************************/
* type of duration

valid range -			had help, valid answer
invalid range (-999) -	had help, invalid answer
missing - 				no help at all
						entire section missing, FQ questionaire
						entire section ineligible, Nursing Home
						entire section skipped, Other reason;

/*******************************************************************************
*** PART 6A: MOBILITY/SELF-CARE, INITIAL ROUND (Round1 & New sample in Round5)
*******************************************************************************/
%macro parta(num);
	
data r&num._duration_init;
	set r&num._needhelp;
	
	/* only keep new sample person in Round 5 */
	%if &num. = 5 %then %do;
		if yearsample = 2015;
	%end;
	
	/* load important dates */
	m = r&num.casestdtmt; /* interview month */
	y = r&num.casestdtyr; /* interview year */
	
	/***************************************************************************
	*** MOBILITY
	***************************************************************************/
	
	/* MO: YES to any help with mobility last month*/
	if mo&num.outhlp = 1 or mo&num.insdhlp = 1 or mo&num.bedhlp = 1 then
	do;
		/* DM2 - more than a year */
		if dm&num.helpyrmor = 1 then
		do;
			mo_help_start_month = m;
			mo_help_start_year = y - 1;
			mo_help_end_month = m;
			mo_help_end_year = y;
		end;
		
		/* DM2 - less than a year */
		else if dm&num.helpyrmor = 2 then
		do;
			mo_help_end_month = m; 
			mo_help_end_year = y;
			
			/* DM3B - month first got help */
			if 12 >= dm&num.mthgethlp >= m and dm&num.mthgethlp > 0 then /* start & end diff year */
			do;
				mo_help_start_month = dm&num.mthgethlp;
				mo_help_start_year = y - 1;
			end;
			
			else if 0 < dm&num.mthgethlp < m then /* start & end same year */                 
			do;
				mo_help_start_month = dm&num.mthgethlp;
				mo_help_start_year = y;
			end;
			
			else if dm&num.mthgethlp in (-7, -8) or dm&num.mthgethlp > 12 then /* RF/DK/invalid month - code last month*/
			do;
				mo_help_start_month = m; 
				mo_help_start_year = y;
			end;
		end;
				
		/* DM2 - RF/DF if more than a year - then only code last month*/
		else if dm&num.helpyrmor in (-7, -8) then
		do;
			mo_help_start_month = m; 
			mo_help_start_year = y;
			mo_help_end_month = m; 
			mo_help_end_year = y;
		end;
	end;
		
		
	/* MO: NO/RF/DK to all help with mobility last month */
	else if mo&num.outhlp in (2, -7, -8) and mo&num.insdhlp in (2, -7, -8) and mo&num.bedhlp in (2, -7, -8) then
	do;
		
		/* DM1 - yes, help last year */
		if dm&num.helpmobil = 1 then
		do;
			
			mo_help_start_month = m;
			mo_help_start_year = y - 1;
			
			/* DM3B - month last got help*/
			if 12 >= dm&num.mthgethlp >= m and dm&num.mthgethlp > 0 then /* start & end same year */
			do; 
				mo_help_end_month = dm&num.mthgethlp;
				mo_help_end_year = y - 1;
			end;
				
			else if 0 < dm&num.mthgethlp < m then /* start & end diff year */
			do; 
				mo_help_end_month = dm&num.mthgethlp;
				mo_help_end_year = y;
			end;
				
			else if dm&num.mthgethlp in (-7, -8) or dm&num.mthgethlp > 12 then /* RF/DK/invalid month */
			do;
				mo_help_end_month = -999;
				mo_help_end_year = -999;
			end;
		end;
		
		/* DM1 - no/RF/DK help last year */
		else if dm&num.helpmobil in (2, -7, -8) then 
		do;
			/* No help at all */
			mo_ever_help = 0;
		end;
	end;
	
	/***************************************************************************
	*** SELF-CARE
	***************************************************************************/

	/* SC: YES to any help with self-care last month*/
	if sc&num.eathlp = 1 or sc&num.bathhlp = 1 or sc&num.toilhlp = 1 or sc&num.dreshlp = 1 then
	do;
		/* DS2 - more than a year */
		if ds&num.hlpmrtnyr = 1 then
		do;
			sc_help_start_month = m;
			sc_help_start_year = y - 1;
			sc_help_end_month = m; 
			sc_help_end_year = y;
		end;
		
		/* DS2 - less than a year */
		else if ds&num.hlpmrtnyr = 2 then
		do;
			sc_help_end_month = m; 
			sc_help_end_year = y;
			
			/* DS3B - month first got help*/
			if 12 >= ds&num.mthgethlp >= m and ds&num.mthgethlp > 0 then /* start & end diff year */
			do;
				sc_help_start_month = ds&num.mthgethlp;
				sc_help_start_year = y - 1;
			end;
			
			else if 0 < ds&num.mthgethlp < m then /* start & end same year */                 
			do;
				sc_help_start_month = ds&num.mthgethlp;
				sc_help_start_year = y;
			end;
			
			else if ds&num.mthgethlp in (-7, -8) or ds&num.mthgethlp > 12 then /* RF/DK/invalid month - code last month*/
			do;
				sc_help_start_month = m; 
				sc_help_start_year = y;
			end;
		end;
				
		/* DS2 - RF/DF if more than a year - then only code last month*/
		else if ds&num.hlpmrtnyr in (-7, -8) then
		do;
			sc_help_start_month = m; 
			sc_help_start_year = y;
			sc_help_end_month = m; 
			sc_help_end_year = y;
		end;
	end;
		
		
	/* SC: NO/RF/DK to all help with self-care last month */
	else if sc&num.eathlp in (2, -7, -8) and sc&num.bathhlp in (2, -7, -8) 
		and sc&num.toilhlp in (2, -7, -8) and sc&num.dreshlp in (2, -7, -8) then
	do;
		
		/* DS1 - yes, help last year */
		if ds&num.gethlpeat = 1 then
		do;
			
			sc_help_start_month = m;
			sc_help_start_year = y - 1;
			
			/* DS3B - month last got help*/
			if 12 >= ds&num.mthgethlp >= m and ds&num.mthgethlp > 0 then /* start & end same year */
			do; 
				sc_help_end_month = ds&num.mthgethlp;
				sc_help_end_year = y - 1;
			end;
				
			else if 0 < ds&num.mthgethlp < m then /* start & end diff year */
			do; 
				sc_help_end_month = ds&num.mthgethlp;
				sc_help_end_year = y;
			end;
				
			else if ds&num.mthgethlp in (-7, -8) or ds&num.mthgethlp > 12 then /* RF/DK/invalid month */
			do;
				sc_help_end_month = -999;
				sc_help_end_year = -999;
			end;
		end;
		
		/* DS1 - no/RF/DK help last year */
		else if ds&num.gethlpeat in (2, -7, -8) then 
		do;
			/* No help at all */
			sc_ever_help = 0;
		end;
	end;
run;

%mend parta;

%parta(1)
%parta(5)


/*******************************************************************************
*** PART 6B: MOBILITY/SELF-CARE, FOLLOW-UP ROUND (2, 3, 4, 5(countinuing sample) 6, 7, 8)
*******************************************************************************/
%macro partb(num, pre);
	
data r&num._duration_follow;
	set r&num._needhelp;
	
	/* only keep continuing sample person in Round 5 */
	%if &num. = 5 %then %do;
		if yearsample = 2011;
	%end;

	/* load important dates - cannot use interview b/c we have FQ sample here */
	if r&num.status = 62 and DEATH_DT > 0 then 
	do;
		m = month(DEATH_DT); /* death month */
		y = year(DEATH_DT); /* deah year */
		m_last = r&pre.casestdtmt; /* interview month - last round*/
		y_last = r&pre.casestdtyr; /* interview year - last round*/
	end;
	
	else do;
		m = r&num.casestdtmt; /* interview month */
		y = r&num.casestdtyr; /* interview year */
		m_last = r&pre.casestdtmt; /* interview month - last round*/
		y_last = r&pre.casestdtyr; /* interview year - last round*/
	end;
	
	/***************************************************************************
	*** MOBILITY
	***************************************************************************/

	/* MO: YES to any help with mobility last month */
	if mo&num.outhlp = 1 or mo&num.insdhlp = 1 or mo&num.bedhlp = 1 then
	do;
		/* YES help in last round - DM3E - CHECK FOR GAP */
		* gap/RF/DK/last information incorrect - only code last month;
		if dm&num.nohelp in (1, -7, -8, 90) then
		do;
			mo_help_start_month = m; 
			mo_help_start_year = y;
			mo_help_end_month = m; 
			mo_help_end_year = y;
		end;
		* no gap - continuous help since last interview;
		else if dm&num.nohelp = 2 then
		do;
			mo_help_start_month = m_last;
			mo_help_start_year = y_last;
			mo_help_end_month = m; 
			mo_help_end_year = y;
		end;
		
		/* NO help in last round - DM3C - CHECK FOR MONTH STARTED */
		* RF/DK/last information incorrect/invalid input - only code last month;
		if dm&num.helpstmo in (-7, -8, 90) or dm&num.helpstmo > 12
			or dm&num.helpstyr in (-7, -8, 90) then
		do;
			mo_help_start_month = m; 
			mo_help_start_year = y;
			mo_help_end_month = m; 
			mo_help_end_year = y;
		end;
		* valid input starting months;
		else if dm&num.helpstmo > 0 then do;
			mo_help_start_month = dm&num.helpstmo;
			mo_help_start_year = dm&num.helpstyr;
			mo_help_end_month = m; 
			mo_help_end_year = y;
		end;
	end;	
		
	/* MO: no/RF/DK to all help with mobility last month */
	else if mo&num.outhlp in (2, -7, -8) and mo&num.insdhlp in (2, -7, -8) and mo&num.bedhlp in (2, -7, -8) then
	do;
		
		/* YES help in last round - DM3D - CHECK FOR MONTH ENDED */
		* RF/DK/last information incorrect/invalid input - code missing duration;
		if dm&num.helpendmo in (-7, -8, 90) or dm&num.helpendmo > 12 
			or dm&num.helpendyr in (-7, -8, 90) then
		do;
			mo_help_start_month = -999;
			mo_help_start_year = -999;
			mo_help_end_month = -999;
			mo_help_end_year = -999;
		end;
		
		* valid input ending months;
		else if dm&num.helpendmo > 0 then 
		do;
			mo_help_start_month = m_last;		
			mo_help_start_year = y_last;
			mo_help_end_month = dm&num.helpendmo;
			mo_help_end_year = dm&num.helpendyr;
		end;
		
		/* NO help in last round - DM1 - CHECK IF EVER HELP */
		* Yes, there is help - DM3C and DM3D (NOTE THIS QUESTION HAS BEEN SKIPPED IN R2 & R3 *******);
		if dm&num.helpmobil = 1 and y not in (2012, 2013) then 
		do;
			if 1 <= dm&num.helpstmo <= 12 
				and 1 <= dm&num.helpendmo <= 12
				and dm&num.helpstyr > 0 
				and dm&num.helpendyr > 0 then 
			do;
				mo_help_start_month = dm&num.helpstmo;
				mo_help_start_year = dm&num.helpstyr;
				mo_help_end_month = dm&num.helpendmo;
				mo_help_end_year = dm&num.helpendyr;
			end;
			else
			do;
				mo_help_start_month = -999;
				mo_help_start_year = -999;
				mo_help_end_month = -999;
				mo_help_end_year = -999;
			end;
		end;
		
		else if dm&num.helpmobil = 1 and y in (2012, 2013) then
		do;
			mo_help_start_month = -999;
			mo_help_start_year = -999;
			mo_help_end_month = -999;
			mo_help_end_year = -999;
		end;
		
		* NO/RF/DK - no help at all;
		else if dm&num.helpmobil in (2, -7, -8) then 
		do;
			/* No help at all */
			mo_ever_help = 0;
		end;
	end;

	/***************************************************************************
	*** SELF-CARE
	***************************************************************************/	
	/* SC: YES to any help with self-care last month */
	if sc&num.eathlp = 1 or sc&num.bathhlp = 1 or sc&num.toilhlp = 1 or sc&num.dreshlp = 1 then
	do;
		/* YES help in last round - DS3E - CHECK FOR GAP */
		* gap/RF/DK/last information incorrect - only code last month;
		if ds&num.nohelp in (1, -7, -8, 90) then
		do;
			sc_help_start_month = m; 
			sc_help_start_year = y;
			sc_help_end_month = m; 
			sc_help_end_year = y;
		end;
		* no gap - continuous help since last interview;
		else if ds&num.nohelp = 2 then
		do;
			sc_help_start_month = m_last;
			sc_help_start_year = y_last;
			sc_help_end_month = m; 
			sc_help_end_year = y;
		end;
		
		/* NO help in last round - DS3C - CHECK FOR MONTH STARTED */
		* RF/DK/last information incorrect/invalid input - only code last month;
		if ds&num.helpstmo in (-7, -8, 90) or ds&num.helpstmo > 12
			or ds&num.helpstyr in (-7, -8, 90) then
		do;
			sc_help_start_month = m; 
			sc_help_start_year = y;
			sc_help_end_month = m; 
			sc_help_end_year = y;
		end;
		* valid input starting months;
		else if ds&num.helpstmo > 0 then do;
			sc_help_start_month = ds&num.helpstmo;
			sc_help_start_year = ds&num.helpstyr;
			sc_help_end_month = m; 
			sc_help_end_year = y;
		end;
	end;	
		
	/* SC: no/RF/DK to all help with self-care last month */
	else if sc&num.eathlp in (2, -7, -8) and sc&num.bathhlp in (2, -7, -8) 
		and sc&num.toilhlp in (2, -7, -8) and sc&num.dreshlp in (2, -7, -8) then
	do;
		
		/* YES help in last round - DS3D - CHECK FOR MONTH ENDED */
		* RF/DK/last information incorrect/invalid input - code missing duration;
		if ds&num.helpendmo in (-7, -8, 90) or ds&num.helpendmo > 12 
			or ds&num.helpendyr in (-7, -8, 90) then
		do;
			sc_help_start_month = -999;
			sc_help_start_year = -999;
			sc_help_end_month = -999;
			sc_help_end_year = -999;
		end;
		
		* valid input ending months;
		else if ds&num.helpendmo > 0 then 
		do;
			sc_help_start_month = m_last;
			sc_help_start_year = y_last;
			sc_help_end_month = ds&num.helpendmo;
			sc_help_end_year = ds&num.helpendyr;
		end;
		
		/* NO help in last round - DS1 - CHECK IF EVER HELP */
		* Yes, there is help - DS3C and DS3D (NOTE THIS QUESTION HAS BEEN SKIPPED IN R2 & R3 *******);
		if ds&num.gethlpeat = 1 and y not in (2012, 2013) then 
		do;
			if 1 <= ds&num.helpstmo <= 12 
				and 1 <= ds&num.helpendmo <= 12
				and ds&num.helpstyr > 0 
				and ds&num.helpendyr > 0 then 
			do;
				sc_help_start_month = ds&num.helpstmo;
				sc_help_start_year = ds&num.helpstyr;
				sc_help_end_month = ds&num.helpendmo;
				sc_help_end_year = ds&num.helpendyr;
			end;
			else
			do;
				sc_help_start_month = -999;
				sc_help_start_year = -999;
				sc_help_end_month = -999;
				sc_help_end_year = -999;
			end;
		end;
		
		else if ds&num.gethlpeat = 1 and y in (2012, 2013) then 
		do;
			sc_help_start_month = -999;
			sc_help_start_year = -999;
			sc_help_end_month = -999;
			sc_help_end_year = -999;
		end;
		
		* NO/RF/DK - no help at all;
		else if ds&num.gethlpeat in (2, -7, -8) then 
		do;
			/* No help at all */
			sc_ever_help = 0;
		end;
	end;	
run;

%mend partb;

%partb(2, 1)
%partb(3, 2)	
%partb(4, 3)
%partb(5, 4)	
%partb(6, 5)
%partb(7, 6)


/*******************************************************************************
*** Step 7: Convert duration start/end month to dates and monthly variables
*******************************************************************************/
%macro prepduration(num);

data r&num._NHATS_analytic_prep;
	
	%if &num. = 1 %then %do;
		set r&num._duration_init;
	%end;
	
	%else %if &num. = 5 %then %do;
		set r&num._duration_init
			r&num._duration_follow;
	%end;
	
	%else %do;
		set r&num._duration_follow;
	%end;
	
	/* convert duration start/end month to dates */
	* valid duration - 		valid dates
	* invalid duration -	missing dates & indicator for got help but unable to determine duration
	* missing duration -	missing dates;

	/* MO */
	if mo_help_start_month = -999 or mo_help_end_month = -999 then
	do;
		mo_unknown_duration = 1;
		mo_start_date = .;
		mo_end_date = .;
	end;
	else if mo_help_start_month > 0 and mo_help_end_month > 0 then
	do;
		mo_unknown_duration = 0;
		mo_start_date = mdy(mo_help_start_month, 1, mo_help_start_year);
		if mo_help_end_month = 12 then mo_end_date = mdy(1, 1, mo_help_end_year+1) - 1; 
		else mo_end_date = mdy(mo_help_end_month+1, 1, mo_help_end_year) - 1; 
	end;
	
	/* SC */
	if sc_help_start_month = -999 or sc_help_end_month = -999 then
	do;
		sc_unknown_duration = 1;
		sc_start_date = .;
		sc_end_date = .;
	end;
	else if sc_help_start_month > 0 and sc_help_end_month > 0 then
	do;
		sc_unknown_duration = 0;
		sc_start_date = mdy(sc_help_start_month, 1, sc_help_start_year);
		if sc_help_end_month = 12 then sc_end_date = mdy(1, 1, sc_help_end_year+1) - 1; 
		else sc_end_date = mdy(sc_help_end_month+1, 1, sc_help_end_year) - 1; 
	end;


	/* MO */
	if mo_start_date > 0 then
	do;
		/* check if indeed valid dates */
		mo_diff_days = mo_end_date - mo_start_date;
		/* dealing with invalid dates - these are either from inconsistent interval
		or the interval inconsistent with the death date 
		R1 - 0, R2 - 0, R3 - 3, R4 - 4, R5 - 1, R6 - 0, R7 - 1, total n = 9 */
		if mo_diff_days < 0 and mo_diff_days ^= . then
		do;
			mo_unknown_duration = 1;
			mo_help = .;
			mo_end_date = .;
			mo_start_date = .;
			mo_diff_days = .;
		end;
	end;	
	
	/* SC */
	if sc_start_date > 0 then
	do;
		/* check if indeed valid dates */
		sc_diff_days = sc_end_date - sc_start_date;
		/* dealing with invalid dates - see above MO counts */
		if sc_diff_days < 0 and sc_diff_days ^= . then
		do;
			sc_unknown_duration = 1;
			sc_help = .;
			sc_end_date = .;
			sc_start_date = .;
			sc_diff_days = .;
		end;
	end;	
run;

/* convert duration start/end month/date to month number 1-108 (1/2010-12/2018) */
data r&num._NHATS_analytic_prep;			
	
	set r&num._NHATS_analytic_prep;

	temp_i_start = (sp_year - 1 - 2010) * 12 + r&num.casestdtmt;
	temp_i_end = (sp_year - 2010) * 12 + r&num.casestdtmt;

	/* scenarios */
	* valid duration - 	valid dates - help months as 1, other months as 0
	* invalid duration -	missing dates & indicator for got help but unable to determine duration
	* missing duration -	missing dates;

	/* MO */
	array mo_help_month {108} mo_help_month_1 - mo_help_month_108;

	* no help at all;
	if mo_ever_help = 0 then
	do i = temp_i_start to temp_i_end;
		mo_help_month(i) = 0;
	end;

	* help cannot be determined (unknown/invalid duration, or FQ/NH/other);
	* (do nothing);
	else if mo_unknown_duration = 1 or mo_start_date = . then 
	do i = temp_i_start to temp_i_end;
		mo_help_month(i) = .;
	end;

	* had help (not necessisarily during post-acute);
	else if mo_start_date > 0 then 
	do;
		do i = temp_i_start to temp_i_end;
			mo_help_month(i) = 0;
		end;

		temp_mo_start_month = month(mo_start_date);
		temp_mo_start_year = year(mo_start_date);
		temp_mo_end_month = month(mo_end_date);
		temp_mo_end_year = year(mo_end_date);
		
		temp_z_start = (temp_mo_start_year - 2010) * 12 + temp_mo_start_month;
		temp_z_end = (temp_mo_end_year - 2010) * 12 + temp_mo_end_month;

		do z = temp_z_start to temp_z_end;
			mo_help_month(z) = 1;
		end;
	end;

	/* SC */
	array sc_help_month {108} sc_help_month_1 - sc_help_month_108;

	* no help at all;
	if sc_ever_help = 0 then
	do i = temp_i_start to temp_i_end;
		sc_help_month(i) = 0;
	end;

	* help cannot be determined (unknown/invalid duration, or FQ/NH/other);
	* (do nothing);
	else if sc_unknown_duration = 1 or sc_start_date = . then 
	do i = temp_i_start to temp_i_end;
		sc_help_month(i) = .;
	end;

	* had help (not necessisarily during post-acute);
	else if sc_start_date > 0  then 
	do;
		do i = temp_i_start to temp_i_end;
			sc_help_month(i) = 0;
		end;

		temp_sc_start_month = month(sc_start_date);
		temp_sc_start_year = year(sc_start_date);
		temp_sc_end_month = month(sc_end_date);
		temp_sc_end_year = year(sc_end_date);
		
		temp_z_start = (temp_sc_start_year - 2010) * 12 + temp_sc_start_month;
		temp_z_end = (temp_sc_end_year - 2010) * 12 + temp_sc_end_month;

		do z = ((temp_sc_start_year - 2010) * 12 + temp_sc_start_month) to ((temp_sc_end_year - 2010) * 12 + temp_sc_end_month);
			sc_help_month(z) = 1;
		end;
	end;

	drop temp_: i z;
run;


%mend prepduration;
%prepduration(1)
%prepduration(2)
%prepduration(3)
%prepduration(4)
%prepduration(5)
%prepduration(6)
%prepduration(7)



/*******************************************************************************
*** Step 8: for each SP in the super set of NHAT SP set, create a LOOKUP 
*** table concatenating all duration of help from R1 to R7
*******************************************************************************/

proc sql;
	create table union_all as
	select * 
	from r1_NHATS_analytic_prep (keep = spid sp_year mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108)
	UNION 
	(select * 
	from r2_NHATS_analytic_prep (keep = spid sp_year mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108))
	UNION 
	(select * 
	from r3_NHATS_analytic_prep (keep = spid sp_year mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108))
	UNION 
	(select * 
	from r4_NHATS_analytic_prep (keep = spid sp_year mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108))
	UNION 
	(select * 
	from r5_NHATS_analytic_prep (keep = spid sp_year mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108))
	UNION 
	(select * 
	from r6_NHATS_analytic_prep (keep = spid sp_year mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108))
	UNION 
	(select * 
	from r7_NHATS_analytic_prep (keep = spid sp_year mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108))
	order by spid, sp_year;
quit;

proc transpose data = union_all out = union_all_trans;
	by spid;
	id sp_year;
	var mo_help_month_1 - mo_help_month_108 sc_help_month_1 - sc_help_month_108;
quit;

data union_all_trans2;
	set union_all_trans;
	max = max(of _2011 - _2017);
	keep spid _name_ max;
run;

proc transpose data = union_all_trans2 out = union_all_trans3;
	by spid;
	id _name_;
	var max;
quit;

data NHATS_sp_duration_lookup;
	set union_all_trans3;
	drop _name_;
run;

/* merge back to each round's data */
%macro lookup(num);

proc sql;
	create table r&num._NHATS_analytic_prep2 as
	select a.*, b.*
	from r&num._NHATS_analytic_prep (drop = mo_help_month_: sc_help_month_:) as a 
		left join 
		NHATS_sp_duration_lookup (rename = (spid = spid_lookup)) as b
		on a.spid = b.spid_lookup
	order by spid;
quit;

%mend lookup;
%lookup(1)
%lookup(2)
%lookup(3)
%lookup(4)
%lookup(5)
%lookup(6)
%lookup(7)


/*******************************************************************************
*** Step 9: combine data created above
*******************************************************************************/
%macro outcome(num);

data r&num._NHATS_analytic_prep3;
	
	set r&num._NHATS_analytic_prep2;
	
	round = SP_YEAR - 2010;
	
	if race in (1, 2) then race2 = race;
	else if race in (3, 5) then race2 = 4;
	else if race = 4 then race2 = 3;
	
	keep spid bene_id sp_year round r&num.dresid 
		age agecat_nhats agecat_mbsf agecat_final BENE_BIRTH_DT DEATH_DT
		gender race race2 marital overall_health 
		needhelp_mo needhelp_sc needhelp_ha needhelp_mc
		mo_: sc_:
		ana_final_wt0
		ana_2011_wt0
		W&num.VARSTRAT W&num.VARUNIT
		YEARSAMPLE
		R&num.:
		ffs;
	
	rename	
			W&num.VARSTRAT = varstrata
			W&num.VARUNIT = varunit
			r&num.dresid = dresid;
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
*** Step 10: add dementia category
*******************************************************************************/
%macro dementia(num);

proc sql;
	create table r&num._NHATS_analytic_dem as
	select a.*, b.r&num.demclas as demclas
	from r&num._NHATS_analytic_prep3 as a left join r&num._dementia as b
		on a.spid = b.spid;
quit;

proc freq data = r&num._NHATS_analytic_dem;
	tables demclas;
run;

%mend dementia;
%dementia(1)
%dementia(2)
%dementia(3)
%dementia(4)
%dementia(5)
%dementia(6)
%dementia(7)


/*******************************************************************************
*** Step 11: add baseline variable from the prior round of survey
*******************************************************************************/

%macro priorround(curr, prev);

proc sql;
	create table r&curr._NHATS_analytic as
	select a.*, 
		b.demclas as baseline_demclas, 
		b.needhelp_mo as baseline_needhelp_mo,
		b.needhelp_sc as baseline_needhelp_sc,
		b.needhelp_ha as baseline_needhelp_ha,
		b.needhelp_mc as baseline_needhelp_mc,
		case when b.needhelp_mo = 1 then 0 when b.needhelp_mo = 0 then 1 else . end as baseline_independent_mo,
		case when b.needhelp_sc = 1 then 0 when b.needhelp_sc = 0 then 1 else . end as baseline_independent_sc,
		case when b.needhelp_ha = 1 then 0 when b.needhelp_ha = 0 then 1 else . end as baseline_independent_ha,
		case when b.needhelp_mc = 1 then 0 when b.needhelp_mc = 0 then 1 else . end as baseline_independent_mc,
		b.overall_health as baseline_overall_health
	from r&curr._NHATS_analytic_dem as a 
		left join 
		r&prev._NHATS_analytic_dem as b
		on a.spid = b.spid;
quit;

%mend priorround;
%priorround(2, 1)
%priorround(3, 2)
%priorround(4, 3)
%priorround(5, 4)
%priorround(6, 5)
%priorround(7, 6)

* add placeholder for r1;
data r1_NHATS_analytic;
	set r1_NHATS_analytic_dem;

	array baseline{10} baseline_demclas baseline_overall_health baseline_needhelp_mc 
						baseline_needhelp_ha baseline_needhelp_sc baseline_needhelp_mo
						baseline_independent_mc 
						baseline_independent_ha baseline_independent_sc baseline_independent_mo;
	do i = 1 to 10;
		baseline(i) = .;
	end;

	drop i;
run;