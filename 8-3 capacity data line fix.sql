use APQP 
go

--select distinct PartNumber
----, SupplierCode, CapSupplierName, TaskStatus
----, SupplierCode, Program, model
--from (
Select distinct 
--coalesce(info.PartSupplierTaskID, cslq.PartSupplierTaskID, csl4.PartSupplierTaskID) as PartSupplierTaskID
info.PartSupplierTaskID
, SupplierCode as SupplierCode
, SupplierName as CapSupplierName
, info.SIE
, info.SIEManager
, info.GSM
, info.GSMManager
, info.PartID
, info.PartNumber as PartNumber
, info.PartDescription
, info.ProgramTag  -- regular, BOM error, missing, subcontract, etc
--, info.TagID
--, info.TagName
, info.PartSupplierID
, info.Model
, info.PartSupplierID
, info.Model
, info.EarliestEffectiveDateInBOM
, info.LatestEndDate as LatestEffectiveDate
, info.EffectiveStatus

, case when info.ProgramTag = 'Subcontract' THEN 'Subcontract'
	when info.ProgramTag = 'Missing' THEN 'Missing'
	WHEN info.ProgramTag = 'New' THEN 'New' 
	WHEN info.ProgramTag = 'BOM Error' THEN 'BOM Error'
	WHEN info.ProgramTag = 'Rework' THEN 'Rework'
	WHEN info.ProgramTag = 'Warp Error' THEN 'Warp Error'
	WHEN info.PartID3 is not null then 'Model 3'
	WHEN info.PartIDS IS not null OR info.PartIDX IS not null or info.PartIDte is not null then 'Existing Programs (S/X/TE)'
END as Program

--, info.PartSupplierTagID
, cslq.LatestUpdateDate as QuoteDate
, cslq.Quoted
, csl4.LatestUpdateDate as Other4CreateDate
, csl4.Measured, csl4.Potential, csl4.Designed, csl4.Theoretical
, info.TaskStatus
,  case when Measured IS NOT NULL 
		then case when cslq.Quoted is NOT NULL 
			then case when Measured >= cslq.Quoted 
					then 'Measured capacity greater than quoted'
					else 'Measured capacity less than quoated' 
					end
				else 'No Quoted data' -- missing quoted data 
				end
		else -- when measured IS NULL
			case when cslq.Quoted is NOT NULL 
				then 'No Measured data'
				else 'No data' 
				end
		end 
	MeasuredStatus
,  case when Designed IS NOT NULL 
		then case when cslq.Quoted is NOT NULL 
			then case when Designed >= cslq.Quoted 
					then 'Designed capacity greater than quoted'
					else 'Designed capacity less than quoated' 
					end
				else 'No Quoted data' -- missing quoted data 
				end
		else -- when quoted IS NULL
			case when cslq.Quoted is NOT NULL 
				then 'No Designed data'
				else 'No data' 
			end
		end 
	DesignedStatus
--select *
from ( --info
	select distinct s.SupplierCode, s.Name as SupplierName
	, pst.PartSupplierTaskID
	, ps.PartSupplierID
	, tag.Tag as ProgramTag
	, p.PartID, p.PartNumber
	, p.Description as PartDescription
	, p.EarliestEffectiveDateInBOM
	, case 
		when GETDATE() between p.EarliestEffectiveDateInBOM and p.LatestEndDate then 'Current' 
		when GETDATE() > p.LatestEndDate then 'Past'
		when GETDATE() < p.EarliestEffectiveDateInBOM then 'Future'
		else 'Dates missing'
	end as EffectiveStatus
	, p.LatestEndDate
	
	--, st.Status as TaskStatus 
	, mx.PartID as PartIDX
	, ms.PartID as PartIDS
	, m3.PartID as PartID3
	, te.PartID as PartIDte
	
	--, t.TagName
	--, t.TagID
	, p.SIEUserID
	, us.DisplayName as SIE
	, usm.DisplayName as SIEManager
	, ub.DisplayName as GSM
	, ubm.DisplayName as GSMManager
	, CASE 
		WHEN ps.PPAPStatus in ('Approved','Interim Approved','A - Interim / Documentation',
			'B - Capacity Improvement','C - Capability Improvement','D - Prototype / Deviation','E - Uncontrolled','Rejected') 
			AND ps.IsPartDefined is null THEN 'Legacy Part'
		WHEN ps.IsPartDefined is null Then 'Part Definition Not Completed' 
		When pst.StatusID = 3 or pst.taskscore = 4 or pst.taskscore1 = 4 Then 'Approved'
		when pst.statusID = 4 or pst.taskscore = 1 or pst.taskscore1 = 1 then 'Rejected'
		When pst.StatusID = 2 Then 'Not Reviewed'	
		When pst.StatusID in (1) Then 'Not Submitted'
		WHEN pst.StatusID = 7 then 'PCR Pending'
		When pst.StatusID is null or pst.StatusID=5 Then 'Not Required'
	End as TaskStatus
	
	, Case when m3.PartID is not null then 'Model 3 (EBOM)'
		when mx.PartID is not null and ms.PartID is not null Then 'Shared S/X'
		When mx.PartID is not null and ms.PartID is null Then 'Model X'
		When mx.PartID is null and ms.PartID is not null Then 'Model S' 
		when te.PartID is not null then 'Tesla Energy'
		else 'Others/Inactive'
	End as Model
	from (
		select *
		from apqp.Part
		where FabricatedFlag = 0
	) p
	join APQP.apqp.Supplier s
	on p.PreferredSupplierID = s.SupplierID
	left join APQP.apqp.PartSupplier ps
	on p.PartNumber=ps.PartNumber and s.SupplierCode=ps.SupplierCode
	left join (
		select *
		from APQP.apqp.PartSupplierTask
		--where TaskName like '%capacity%'
		where TaskID in (40) --- taskid = 40 is for capacity study tasks, and 15 = old capacity study R@R tasks
	) pst
	on ps.PartSupplierID = pst.PartSupplierID
	left join (
		select *
		from (
			select PartSupplierID
			, StatusID
			, TaskScore, TaskScore1
			, ROW_NUMBER() over(partition by PartSupplierID order by coalesce(ModifyDate, CreateDate) desc) as 'rank'
			
			FROM apqp.PartSupplierTask as pst with (NOLOCK)
			where pst.TaskID in (40)
		) x
		where rank = 1
		--and (pst.TaskScore is not null or pst.TaskScore1 is not null)
		--(TaskName like '%capacity%' or (TaskName like '%run%' and TaskName like '%rate%'))
		--and 
		--group by PartSupplierID
	) as cap 
	on ps.PartSupplierID=cap.PartSupplierID
	-- and pst.TaskID = 40
	left join (
		select *
		from apqp.PartTag
		where Enabled = 1
	) pt
	on ps.PartID = pt.PartID
	--left join apqp.Tag t
	--on t.TagID = pt.TagID
	left join apqp.PartSupplierTag tag
	on tag.PartSupplierTagID = ps.PartSupplierTagID
	left join apqp.Status st
	on pst.StatusID = st.StatusID
	left join APQP.apqp.[User] us
	on us.UserID = p.SIEUserID
	left join APQP.apqp.[User] usm
	on us.ManagerUserID = usm.UserID
	
	left join (
		select *
		from APQP.apqp.[User] 
		where SAPUserID!='' and UserID not in ('2586', '2583', '2584')
	)ub
	on ub.SAPUserID = p.BuyerCode
	left join APQP.apqp.[User] ubm
	on ub.ManagerUserID = ubm.UserID
	
	
	---------- assgining program ----------
	left join (
		select PartID 
		from (
			select PartID, [enabled], CreateDate
			, ROW_NUMBER () OVER (PARTITION BY PartID ORDER BY CreateDate DESC) as 'Rank' 
			from apqp.PartTag with (NOLOCK) 
			where TagID = 31 --- for model S
		)x 
		where [RANK] = 1 AND [enabled] = 1
	) as ms 
	on ms.PartID=p.PartID
	left join (
		select PartID 
		from (
			select PartID, [enabled], CreateDate
			, ROW_NUMBER () OVER (PARTITION BY PartID ORDER BY CreateDate DESC) as 'Rank' 
			from apqp.PartTag with (NOLOCK) 
			where TagID=30
		)x 
		where [RANK] = 1 AND [enabled] = 1
	) as mx 
	on mx.PartID=p.PartID
	left join (
		select PartID 
		from (
			select PartID, [enabled], CreateDate
			, ROW_NUMBER () OVER (PARTITION BY PartID ORDER BY CreateDate DESC) as 'Rank' 
			from apqp.PartTag with (NOLOCK) 
			where TagID=124
		)x 
		where [RANK] = 1 AND [enabled] = 1
	) as m3 
	on m3.PartID=p.PartID
	left join (
		select PartID 
		from (
			select PartID, pt.CreateDate
			,ROW_NUMBER () OVER (PARTITION BY PartID ORDER BY pt.CreateDate DESC) as 'Rank' 
			from apqp.PartTag pt with (NOLOCK) 
			join apqp.tag t with (NOLOCK) on pt.tagid  = t.tagid 
			where pt.[enabled] = 1 and t.[enabled] = 1 and t.tagname like '%Tesla Energy%' and t.tagtypeid = 2
		)x 
		where [RANK] = 1 
	) as te on te.PartID=p.PartID
	
	  where s.SupplierCode is not null
	  and (mx.PartID is not null or ms.PartID is not null or m3.partid is not null
	   or tag.Tag is not null or te.partid is not null)
) info 
left outer join (--cslq
-------------- get contract amount ------------------------
	select *
	from (-- contra
		select distinct 
		--CapacityStudyLineID, 
		PartSupplierTaskID, Quoted, ApprovalStatus
		, coalesce(ModifyDate, CreateDate) LatestUpdateDate
		, CapacityType
		, ROW_NUMBER() over(partition by PartSupplierTaskID order by ModifyDate desc, CreateDate desc) rank
		from APQP.apqp.CapacityStudyLine
		where CapacityType = 'Contract' and (Quoted is not null)
	) contra
	--on contra.PartSupplierTaskID = csl.PartSupplierTaskID
	where rank = 1
) cslq --capacitystudyline quote
on cslq.PartSupplierTaskID = info.PartSupplierTaskID
left outer join ( --csl4
--------------------- get the other 4 amount -----------------------
	select *
	from ( --cc
		select distinct 
		--CapacityStudyLineID, 
		PartSupplierTaskID, ApprovalStatus
		, coalesce(ModifyDate, CreateDate) LatestUpdateDate
		, Measured, Potential, Designed, Theoretical, CapacityType
		, ROW_NUMBER() over(partition by PartSupplierTaskID order by ModifyDate desc, CreateDate desc) rank
		from APQP.apqp.CapacityStudyLine
		where CapacityType = 'Current Capacity'
		and ((Measured is not null) or (Potential is not null) or (Theoretical is not null) or (Designed is not null))
	) cc
	where rank = 1
) csl4 --capacitystudyline other4
on info.PartSupplierTaskID = csl4.PartSupplierTaskID
--) x
--where csl4.Measured is not null
--where SupplierCode is null
--and PartNumber = '1117083-00-B'
--where x.TaskStatus = 'Rejected'
--where x.Program in ('Missing','New','Subcontract', 'Model 3')
--and (TaskStatus in ('Part Definition Not Completed'))
-- or TaskStatus is null)
--and x.PartNumber = '1004838-00-B'
--group by TaskStatus
--order by count(PartNumber)