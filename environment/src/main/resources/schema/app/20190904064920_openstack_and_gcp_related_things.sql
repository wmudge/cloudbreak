-- // openstack and gcp related things
-- Migration SQL that makes the change goes here.

ALTER TABLE environment_network ADD COLUMN IF NOT EXISTS sharedprojectid varchar(255);
ALTER TABLE environment_network ADD COLUMN IF NOT EXISTS networkingoption varchar(255);
ALTER TABLE environment_network ADD COLUMN IF NOT EXISTS publicnetid varchar(255);
ALTER TABLE environment_network ADD COLUMN IF NOT EXISTS routerid varchar(255);

-- //@UNDO
-- SQL to undo the change goes here.

ALTER TABLE environment_network DROP COLUMN IF EXISTS sharedprojectid;
ALTER TABLE environment_network DROP COLUMN IF EXISTS networkingoption;
ALTER TABLE environment_network DROP COLUMN IF EXISTS publicnetid;
ALTER TABLE environment_network DROP COLUMN IF EXISTS routerid;
