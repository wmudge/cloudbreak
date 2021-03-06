base:
           '*':
             - nginx
             - tags
             - fluent
             - ntp
             - freeipa
             - freeipa.services
             - dns

           'roles:freeipa_primary':
             - match: grain
             - freeipa.primary-install
             - freeipa.common-install
             - freeipa.backups

           'roles:freeipa_replica':
             - match: grain
             - freeipa.replica-install
             - freeipa.common-install
             - freeipa.backups

           'roles:freeipa_primary_replacement':
             - match: grain
             - freeipa.replica-install
             - freeipa.common-install
             - freeipa.promote-replica-to-master
             - freeipa.backups
