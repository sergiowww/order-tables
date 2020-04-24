select 
	isf.REF_NAME
from 
	information_schema.INNODB_SYS_FOREIGN as isf inner join
	information_schema.INNODB_SYS_FOREIGN_COLS isfc on(isfc.id = isf.id)
where 
	isf.id like ? and
	isf.for_name like ? and
	isfc.for_col_name = ?