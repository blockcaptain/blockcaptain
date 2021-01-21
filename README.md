# BlockCaptain

![CI](https://github.com/blockcaptain/blockcaptain/workflows/CI/badge.svg?branch=master)

BlockCaptain is an open source data integrity systems for Linux. 

This project attempts to rigerously evaluate and mitigate failures modes which may lead to data loss. The probability of major data loss should be close to 0%. Every risk that can be reasonably mitigated should be, for example:

 * silent data corruption (bitrot) is detected with regular scrubs and monitoring kernel events (btrfs silent correction on raid1)
 * complete failure of all physical disks is mitigating using encrypted content addressed cloud backup 
 * unrecoverable data bug in the cloud backup tool is mitigating by supporting multiple tools (e.g. restic + kopia) in parallel
 * forgetting to pay the bill on your cloud storage is mitigated by monitoring billing state of cloud accounts
 * malicious corruption of data is mitigated with regular snapshot backups and privilege segregation (e.g. the host only has quota-limited append access to the backup targets)
 * failure to run or send alerts is mitigated with external scheduled monitors (e.g. healthchecks.io)

[Roadmap](https://github.com/blockcaptain/blockcaptain/milestones)

[Contributing](https://blockcaptain.dev/development/contributing/)

## Status

The project is in early alpha stage. The features for the first release were complete in milestone 0. [Milestone 1](https://github.com/blockcaptain/blockcaptain/milestone/2) is underway to harden the entire system with unit and integration tests. Completion of [milestone 2](https://github.com/blockcaptain/blockcaptain/milestone/7) will result in a 1.0 release.
