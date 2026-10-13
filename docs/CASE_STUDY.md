# Case study: condition monitoring as a streaming pipeline

> **All data in this repository is synthetic.** The machines, their telemetry, the faults and the thresholds are
> generated or chosen for a demonstration. Nothing here was measured on a real machine.

This document discusses what the repository does, why it is built the way it is, how it would apply to a real
plant, and what data engineering contributes to the problem. The facts it relies on are in
[ARCHITECTURE.md](ARCHITECTURE.md) and in the benchmark's
[closure report](internal/phase3_task3.3_closure_report.en.md). It contains no economic or market figures: none
were measured, and none are assumed.

## 1. The problem

Several common faults of rotating machines such as motors, pumps and compressors show in measurable signals
before the machine stops: an imbalanced rotor raises vibration; a failing bearing heats up; a machine working
against a blockage draws more current. The signals that
reveal these conditions are the ones this repository simulates: bearing temperature, vibration velocity, motor
current and speed.

Where a plant already collects such signals, in a control system, a historian or a handheld survey on a round,
what decides their value is the path that turns a reading into a decision in time. A reading that sits in a historian until
someone opens a trend helps explain a failure afterwards; it does not prevent it. The cost of late detection is
not quantified here, because nothing in this repository measures it. Its form is familiar: an unplanned stop
instead of a planned repair, secondary damage when a bearing is allowed to fail, and work that has to be done at
the worst moment instead of the next convenient one.

The problem this repository addresses is therefore narrower than "predictive maintenance". It is the path from a
sensor reading to an operator who can trust what is on the screen: every reading delivered, in order, evaluated
the same way every time, with an alarm that means what it says.

## 2. Why a streaming pipeline and not a batch report

A daily or hourly report answers "what happened". A stream answers "what is happening", and the difference
matters when a condition develops in minutes. In this repository's simulation, the bearing-overheating fault ramps
the temperature by 45 °C over 180 s; the warning and the critical alarm for compressor-01 in the first dashboard
capture were raised 58 s apart. A report run every hour would have shown both at once, after the fact.

Measured on one laptop with synthetic data (conditions in the [README](../README.md#measured-figures)), the time from
a sample to its alarm event being acknowledged by Kafka had a median of 35 ms at 50 messages per second. On that
scale, the time an alarm takes is set by its persistence rule (several consecutive samples, one per second), not by
the pipeline. That is the intended balance: the pipeline adds milliseconds, and the alarm logic decides how much
evidence an alarm needs.

Streaming also changes what can be guaranteed. A batch job reads a file that exists; a stream must decide what
happens to a reading when a component is slow, stopped or restarting. Most of the engineering in this repository
is about those decisions.

## 3. What was built, and which decisions answer plant problems

The pipeline is described in [ARCHITECTURE.md](ARCHITECTURE.md). Four design decisions correspond directly to
problems a plant has.

**Order per machine.** An alarm is a sequence of events: raised, then cleared. If a reader sees them out of order,
it shows the wrong state. Kafka orders records only within a partition, so every record of a machine must land in
the same partition. Telemetry is keyed by machine. Alarm events are keyed by alarm, which compaction needs, but
partitioned by machine (section 5 of the architecture document). With Kafka's default partitioner, one machine's
alarms spread over partitions; in a replay measured during the project, only 19 of 48 alarms were kept
after a replay. An integration test now requires every alarm of a machine to share one partition.

**Persistence and hysteresis.** A reading that crosses a threshold once is noise as often as it is a fault. Each
alarm raises only after several consecutive samples above its threshold (three for temperature, five for vibration
and current) and clears only after as many samples below the threshold minus a deadband. This is the standard
treatment against alarms that chatter, in the style of ANSI/ISA-18.2, and it is the reason the alarm takes seconds
while the pipeline takes milliseconds.

**Delivery without silent loss.** The benchmark found that Mosquitto, with its default settings, dropped messages
for a subscriber that fell behind for under a second, while still acknowledging them to the publisher. Nothing
reported it. The fix was a larger, bounded queue and a monitor that logs every drop with its count. The same
attitude applied at the other end: ThingsBoard's default queue gave each batch 2 s and three retries, and under load
it dropped alarm events; in one test only 43 of 48 alarms arrived. The pipeline devices now use a sequential queue
with a 60 s batch and 20 retries, and every full-stack test session fails if ThingsBoard dropped a message.

**Reconciliation and restart.** A pipeline that runs for months will be restarted. The alarm consumer rebuilds its
state from the compacted alarm topic at start-up. Two defects in that path were found during the project's architecture review and the fix that followed:
a restart at the wrong moment could clear an alarm that was still active, or raise a phantom one. Both were fixed by
a replay guard and are covered by tests that fail on the old code. Separately, `make up` reconciles ThingsBoard
with Kafka, clearing alarms that ThingsBoard shows as active but Kafka no longer holds.

Around these decisions, the pipeline keeps a contract: Avro schemas registered in Schema Registry, shared by the
Python and Node.js components; a dead-letter topic that keeps every rejected message intact with a reason; alarm ids
derived from the data, so that processing the same samples twice gives the same id.

## 4. What the benchmark showed, and what it does not

The benchmark measured latency at 5 and 50 messages per second and searched for the highest sustained rate. On
one laptop, with every component on one host, a single Kafka broker writing to memory and synthetic messages of
about 218 bytes, the pipeline sustained 2000 messages per second in five of five runs of 300 s. The step above,
3000, could not be measured validly, because the load generator reached its own CPU limit.

For scale: the simulator publishes one message per machine per second by default, so 2000 messages per second is
the rate of 2000 such machines. That comparison is arithmetic, not a measurement of a plant.

What the figures do not show is as important. They say nothing about separate machines or a real network. Kafka
on memory with one broker makes acknowledgements cheaper than a replicated, disk-backed cluster would. The windows
are 300 s, not days. The path into ThingsBoard, limited by configuration to 10 messages per second per machine, was
not measured at all. Section 4 of the closure report lists these limits; they apply to every figure.

## 5. Applying it to a real plant

**What is replaced.** The simulator is replaced by real sources: sensors on the machines, read by edge gateways
or by the control system, publishing over MQTT or a protocol that a gateway converts to it. The message contract
(machine id, sequence number, timestamp, channels) stays; whatever publishes must meet it, or its messages end in
the dead-letter topic with a reason.

**What stays.** The bridge's validation, the schemas and their compatibility rule, the alarm engine with
persistence and hysteresis, the order-per-machine contract, the dead-letter path, the structured logs and the
reconciliation. These are independent of where the data comes from.

**What is missing,** and is listed in section 13 of the architecture document:

- *Security.* MQTT is anonymous, Kafka is PLAINTEXT and there is no TLS. A plant network needs authentication and
  encryption on every hop.
- *Availability.* Every component is a single instance. A real deployment needs replicated Kafka on persistent
  storage, more than one broker, and an alarm consumer that can survive the loss of an instance; today the consumer
  supports one instance only.
- *Delivery before the bridge.* Messages published while the bridge is down are lost. A persistent MQTT session, or
  buffering at the gateway, would be required.
- *Retention.* Kafka here keeps its data in memory (tmpfs) and empties on restart. A plant needs a retention policy and, for
  history, a store designed for it.
- *Thresholds.* The thresholds here are illustrative. Real ones come from the machine's class and mounting, the
  manufacturer, and the plant's own history, set and reviewed by the people who maintain the machines.
- *Clocks.* The replay guard trusts the producers' timestamps. Real sources need synchronised clocks, or the
  skipped-sample counter becomes an alarm of its own.

## 6. What data engineering contributes

The alarm logic in this repository is simple. The engineering is in making it trustworthy, and three examples from
the project show what that means in practice.

**Measuring before claiming.** The pipeline was tested and looked healthy before the benchmark. Only measuring at
load showed that Mosquitto dropped acknowledged messages, that a healthcheck loaded the broker it was checking, and
that Docker Desktop killed containers mid-stop. None of these would have appeared in a demonstration; all of them
would have appeared in a plant.

**Testing the tests.** During the project, a unit test of the thermal model was found to pass for any time
constant, because it started at the value it was meant to reach. A test that cannot fail is a claim without
evidence. The defects of the alarm restore were fixed only after tests had been written that fail on the old code
and pass on the new one, and that was shown in both directions.

**Writing down the guarantees.** Each hop states what it guarantees and what it does not: at most once before the
bridge, once from the bridge to Kafka unless the bridge crashes, at least once to the alarm topic, at least once to ThingsBoard's transport.
A maintenance team can decide whether that is good enough only if it is written down; an undocumented "it works"
cannot be checked.

These are the same habits a plant expects of its instruments: calibrate them, know their limits, and record what
they say. Applied to the data path, they turn a set of connected tools into a system an operator can rely on.
