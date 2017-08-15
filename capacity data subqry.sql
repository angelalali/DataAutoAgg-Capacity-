select distinct PartNumber, SupplierCode
, TaskStatus, Program
from 
(
select distinct s.SupplierCode, s.Name as SupplierName
	, pst.PartSupplierTaskID
	, ps.PartSupplierID
	, tag.Tag as ProgramTag
	, p.PartID, p.PartNumber
	, p.Description as PartDescription
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
		select PartSupplierID
		, StatusID
		, TaskScore, TaskScore1
		, ROW_NUMBER() over(partition by PartSupplierID order by coalesce(ModifyDate, CreateDate) desc) as 'rank'
		
		FROM apqp.PartSupplierTask as pst with (NOLOCK)
		where pst.TaskID in (40)
		and (pst.TaskScore is not null or pst.TaskScore1 is not null)
		--(TaskName like '%capacity%' or (TaskName like '%run%' and TaskName like '%rate%'))
		--and 
		--group by PartSupplierID
	) as cap 
	on ps.PartSupplierID=cap.PartSupplierID and pst.TaskID = 40
	--and pst.TaskID = 40
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
	   --and p.FabricatedFlag = 0
) x

--where TaskStatus = 'Legacy Part'
--where PartNumber in 
--(

--)
order by PartNumber