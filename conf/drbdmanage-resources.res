# ============================================================
# DRBDMANAGED CONFIGURATION
# ============================================================
# ============================================================
# STORAGE MANAGER CLASS
#
# drbdmanage will use this class to allocate local storage
# for DRBD volumes
# ============================================================
storage-plugin = drbdmanage.storage.lvm.LVM
# ============================================================
# GREATEST VALID NODE ID
#
# Node ids range from 0 to max-node-id. drbdmanage will not
# allocate node ids greater than max-node-id
#
# (This is commonly the number of nodes minus 1)
# ============================================================
max-node-id = 31
# ============================================================
# MINIMUM MINOR NUMBER
#
# drbdmanaged will leave numbers less than the minimum minor
# number free for manual allocation
# ============================================================
min-minor-nr = 10
# ============================================================
# DRBD PORT RANGE
#
# drbdmanage will allocate port numbers in the range from
# min-port-nr to max-port-nr
# ============================================================
min-port-nr = 7700
max-port-nr = 7899
# ============================================================
# Path for drbdadm configuration files (*.res, commonly)
# ============================================================
drbd-conf-path = /var/drbd.d/
