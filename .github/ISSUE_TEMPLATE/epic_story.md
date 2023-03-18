---
name: Epic/Story Issue Template
about: Template for "top" level issues - Epics (>2 weeks) / Stories (<2 weeks)
title: 'Epic: New Feature'
labels: epic
assignees:

---

## Summary / Background
What do you want to achieve, why? business context
...

## Scope

What will be impacted and what won't be?
What needs implementation and what is invariant?
e.g.
- user should be able to run workflow X from UI
- enable workflow Y, Z from CLI

## Assumptions
Product / UX assumptions as well as technical assumptions / limitations
e.g.
* Support only Python Runtime
* Focus on DVC experiments only
* Deployment environments don't change often and can be picked up from shared configuration

## Open Questions
e.g.
- How should access control work for shared artifacts (workflow X)
- Python runtime assumption - is it really valid? in light of <...>

## Blockers / Dependencies
List issues or other conditions / blockers

## General Approach
Invocation example:
```shell
$ mapper-run task.tar.gz --ray-cluster <ip>:<port>
```

## Steps

### Must have (p1)
- [ ] subissue2
- [ ] step 2
 - info
 - info

### Optional / followup (p2)
- [ ] ⌛ step 3 wip
- [ ] step 4

## Timelines

Put your estimations here. Update once certainty changes
- end of week (Feb 3) for prototype with workflows X, Y
- Feb 15 - MVP in production
- Low priority followups can be done later
