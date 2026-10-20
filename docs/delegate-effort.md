# Delegate effort and Grok selection

`delegation.candidate_effort_caps` maps candidate patterns to host-owned automatic
worker effort ceilings. For example:

```yaml
delegation:
  candidate_effort_caps:
    "*/gpt-6.1-sol": low
```

All matching caps apply after candidate selection and before configuring its native
adapter or proxy and calling its lifecycle start hook. A cap never raises effort.
Unmatched workers and parent sessions retain their existing policy. This covers
automatic planner dispatch, resume and finish dispatches without an effort hint.
Lower delegate hints also remain lower than `effort.default`: the default supplies
the baseline before a hint caps it, never replacing `hints.effort: low` with medium.
The lifecycle hook remains authoritative and can reject an unsupported/unconfirmed
native setting. Deploy the cap with that hook's contract; model-written hints alone
are insufficient for automatically selected candidates.

The built-in Grok 4.6 and 4.7 presets both use the documented frontier cost rank 5.
Equal-priced 4.6 is no longer a cheaper automatic demotion target. Explicit model
selections remain available, and unavailable candidates are never forced into use.
Grok 4.7 supports low, medium, high and xhigh effort and a 500,000-token context
window, per the [provider guide](https://docs.x.ai/developers/grok-4-7).
The proxy confirms effort only when an accepted request carries a recognized
value; an omitted field or Auto does not prove the provider's actual default.

These changes require a new router process. A running parent can immediately
recover rejected Sol starts by sending `hints.effort: low`, collecting the actual
delegate result, and checking the linked child before calling the work active.
Do not retry an identical hook rejection or count a queued background ID as a child.
