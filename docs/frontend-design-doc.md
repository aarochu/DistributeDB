# DistributeDB frontend design document

Build a high-quality frontend for DistributeDB, a single-primary replicated key-value database written in Rust using only the standard library.
This is a serious systems-engineering project.
The website must look like an industrial observability console, systems debugger, and database-internals inspection tool.
It must NOT look like:

* an AI startup landing page
* a generic SaaS admin dashboard
* a Tailwind template
* a crypto dashboard
* a cyberpunk HUD
* a Vercel clone
* an AI-generated portfolio website

The design should feel intentional, authored, restrained, technical, dense, and mature.
The visual reference point is closer to:
internal infrastructure tooling + database debugger + operating-system instrumentation + research systems console
than a commercial analytics dashboard.
NON-NEGOTIABLE DESIGN RULES
These requirements are strict.
Do not reinterpret them creatively.
1. Monochrome only
The entire website is monochrome.
Use:

* black
* near-black
* graphite
* dark gray
* medium gray
* off-white
* white

Do not introduce:

* green
* red
* blue
* cyan
* purple
* orange
* yellow
* gradients using color
* neon accents

Do not use conventional green/red traffic-light status indicators.
System state must instead be communicated using:

* wording
* luminance
* opacity
* line style
* texture
* shape
* typography

Examples:
HEALTHY
SYNCED
LAGGING
DISCONNECTED
A healthy item may be brighter.
A lagging item may use a hatched indicator or reduced brightness.
A disconnected item may use an outlined or interrupted shape.
An error may use high-contrast white-on-black, a boxed warning, or a distinctive symbol.
Color must never carry system-state meaning.
This requirement applies to:

* node state
* charts
* badges
* warnings
* errors
* buttons
* diagrams
* animations
* hover states

2. NO GENERIC AI-WEBSITE VISUAL LANGUAGE
Absolutely do not add any of the following unless explicitly requested:

* glowing colored orbs
* green/red status dots
* rainbow gradients
* purple gradients
* blue gradients
* glassmorphism
* blurred translucent cards
* massive rounded cards
* pill-shaped everything
* giant floating cards
* decorative blobs
* floating particles with no system meaning
* generic abstract wave backgrounds
* mouse-following spotlights
* parallax backgrounds
* 3D card tilt
* animated borders
* glowing buttons
* huge marketing typography
* fake terminal windows
* fake code snippets
* meaningless hexadecimal decorations
* random charts added for visual interest
* “AI-looking” network graphics
* excessive icons
* emoji
* generic line icons beside every heading
* oversized KPI tiles
* fake metrics
* invented database functionality
* testimonials
* pricing
* sign-in
* account menus
* notifications
* user avatars
* search bars unless required for the Key Explorer
* command palettes
* onboarding prompts
* marketing CTAs

Do not invent features.
If a component does not correspond to actual DistributeDB functionality described in this specification, do not add it.
3. RESTRAINT
The site should feel deliberately under-designed rather than excessively decorated.
Every visual element must have a reason to exist.
Prioritize:

1. hierarchy
2. typography
3. alignment
4. spacing
5. information density
6. system state
7. diagrams
8. data visualization

Do not try to make every part of the page visually impressive.
There should be only one major decorative/expressive visual element:
the animated database-system visualization described below.
Everything else should be calm.
DESIGN SYSTEM
Background
Use a nearly black background.
Suggested foundation:
background: #090909
Primary panel:
#0D0D0D
Secondary raised surface:
#111111
Hairline border:
#262626
Strong border:
#3A3A3A
Primary text:
#E8E8E8
Secondary text:
#A1A1A1
Muted text:
#666666
Bright machine value:
#F5F5F5
Do not use visible colorful gradients.
If depth is required, use only very subtle monochrome radial falloff.
No glow should be visibly colored.
TYPOGRAPHY
Typography is one of the main visual devices.
Use two families maximum.
Use a clean restrained sans-serif for:

* headings
* navigation
* explanatory text
* labels

Use a technical monospace font for:

* LSNs
* node identifiers
* addresses
* metrics
* keys
* values
* latency
* byte counts
* command names
* WAL information
* table values

Suggested character:
Sans:
Inter, Geist, Helvetica Neue, or similar.
Mono:
IBM Plex Mono, JetBrains Mono, Geist Mono, or similar.
Do not use novelty fonts.
Do not make headings gigantic.
Suggested scale:
11px — metadata / overlines
12px — secondary machine labels
13px — table/interface text
14px — body/interface text
16px — subsection titles
18–20px — major panel titles
28–36px — only the main page title or extremely important values
Do not create 60–100px marketing headlines.
Use typography rather than cards to establish hierarchy.
SPACING
Use disciplined spacing.
Base spacing unit around 4px.
Common gaps:
4
8
12
16
24
32
48
Do not create arbitrary giant empty sections.
This is an engineering application, not a luxury marketing site.
Use generous breathing room around major regions while keeping metric areas dense.
CORNERS AND BORDERS
Avoid the rounded-card aesthetic.
Preferred radius:
0–4px for most components.
6px maximum where necessary.
Some panels may have square corners.
Use thin 1px borders.
Do not give every textual item a border.
Do not wrap every metric in its own card.
PAGE STRUCTURE
Build one continuous dashboard page.
Do not create unnecessary separate routes.
Top-level organization:

1. Header
2. Cluster overview
3. System summary strip
4. Replication
5. Throughput + latency
6. Durability + WAL
7. Storage engine
8. Lock contention
9. Recovery
10. Key Explorer
11. Write Console
12. Fault-Tolerance Demo

The page should tell the story of the system from top to bottom:
system exists → traffic enters → writes become durable → logs replicate → storage changes → system crashes/restarts → state survives
HEADER
Make the header extremely restrained.
Example:
DISTRIBUTEDB
Cluster
Replication
Storage
Explorer
Demo
Right side:
3 / 3 NODES
HEALTHY
Do not put a colored circle beside HEALTHY.
A tiny monochrome symbol is acceptable, such as:
●
○
◇
■
but status must also be written explicitly.
Use a thin bottom border.
Header height should be compact.
No giant logo.
No giant navigation pills.
No GitHub star widgets.
OPENING / HERO REGION
Do not build a traditional marketing hero.
No:
"THE FUTURE OF DISTRIBUTED STORAGE"
No:
"Build resilient systems at scale."
No generic subtitle.
Instead, begin as if the user has opened a sophisticated systems instrument.
Example structure:
DISTRIBUTEDB
Single-primary replicated key-value store
Rust / std-only
storage: LSM
durability: group commit
nodes: 3
sync replicas: 1
Beside or immediately below this information, place the signature systems animation.
SIGNATURE ANIMATION
Create one sophisticated Three.js/WebGL visualization inspired by a dense connected systems structure.
Do NOT render:

* a brain
* an AI neural network
* a globe
* a generic glowing orb
* random floating particles

The visualization must conceptually represent DistributeDB's write, WAL, and replication architecture.
Composition
Create three spatial regions:
PRIMARY
REPLICA 01
REPLICA 02
Do not present them as literal rectangular cards floating in 3D.
Instead represent each as a sparse local constellation / structured cluster of monochrome points.
The PRIMARY cluster should be slightly denser and visually dominant.
Replicas should be spatially distinct.
Thin lines connect local points.
A small number of structural lines connect the primary region to replicas.
The entire visualization should feel like a technical model or instrumentation rendering.
Not science fiction.
REPRESENTING WRITES
When a simulated or real write occurs:

1. a small white signal enters the primary
2. it passes through a central ordered path representing sequencing
3. a short pulse moves through the WAL structure
4. after durability, a pulse travels toward each replica
5. replicas acknowledge by briefly increasing local luminance

Keep this extremely subtle.
Do not create explosions.
Do not create glowing multicolored particles.
Signals should look like tiny white impulses moving through a black technical field.
REPRESENTING WAL
The primary cluster should contain a visibly ordered linear structure representing the log.
Think:
small discrete points arranged along a line or arc.
Do not label every point.
When `current_lsn` moves, extend or advance activity along the structure.
`durable_lsn` should visually trail or match it.
This is an abstraction.
It does not need to draw every WAL entry.
REPRESENTING REPLICATION LAG
If a replica becomes behind:

* its signal frequency decreases
* its structural connections become dimmer
* perhaps its cluster drifts very slightly visually quieter
* the textual dashboard reports the actual lag

When disconnected:

* connection line becomes interrupted/dashed
* replica region becomes substantially dimmer
* signals stop reaching it

When reconnecting:

* signals begin traveling again
* activity briefly becomes faster as catch-up occurs
* the replica returns to normal luminance once caught up

Do not use red, orange, or green.
MOVEMENT
The animation must be slow.
Almost architectural.
Use:

* tiny node breathing
* extremely slow structural rotation or camera drift
* occasional signal movement
* subtle depth parallax

Do not make it constantly energetic.
The user should be able to stare at it without distraction.
Movement should suggest:
continuous system operation
not:
cyberpunk spectacle.
CAMERA
The camera may move imperceptibly.
Do not use aggressive orbit controls by default.
Do not make the visualization spin like a 3D product viewer.
If pointer movement affects the camera, limit it to an extremely subtle parallax shift.
The user should never feel that the entire interface is moving.
ANIMATION COLOR
Strict monochrome.
Nodes:
#D8D8D8 through #FFFFFF
Dim nodes:
#555555 through #888888
Connections:
white at approximately 5–20% opacity
Signals:
near-white
Background:
near-black
No cyan.
No violet.
No green.
No red.
No hue changes.
ANIMATION GLOW
Minimal.
Use luminance, not neon bloom.
Small signals may have a tiny white bloom.
Nodes should primarily look like points, not glowing stars.
Avoid additive-blending overload.
Large hazy glow fields are prohibited.
ANIMATION INTEGRATION
The animation should not occupy the entire page.
Do not turn the whole website into a canvas.
It should live inside a defined opening systems region.
Approximately:
40–55% of viewport width on large screens
and approximately:
380–520px tall
depending on layout.
It should feel integrated into the interface architecture.
No giant full-screen splash screen.
CLUSTER OVERVIEW
Directly communicate topology.
Suggested conceptual layout:
PRIMARY
node-0
LSN 184229

```
      ├──────── REPLICA-01
      │         LSN 184229
      │         SYNCED
      │
      └──────── REPLICA-02
                LSN 184184
                LAG 45
```

Do not use rounded colored cards.
Use:

* alignment
* hairline connectors
* labels
* indentation
* grid layout
* subtle region separation

Each node needs:
role
node name
status
uptime
storage engine
durability mode
LSN
replication lag where relevant
Possible states:
ONLINE
SYNCED
LAGGING
DISCONNECTED
Use text.
Never communicate state only with a symbol.
SYSTEM SUMMARY
Below cluster overview, provide one compact status strip.
Example:
OPS/S
842
WRITE P99
6.82 ms
DURABLE LSN
184,229
REPLICAS
2 / 2
WAL SYNC AVG
2.10 ms
These should belong to one horizontal structure.
They are NOT individual cards.
Use thin vertical separators.
REPLICATION
This is one of the main sections.
Large heading:
REPLICATION
Small technical description:
Primary durable position against replica applied positions.
Show:
primary durable_lsn
replica applied_lsn
lag per replica
replicas_connected
Primary current values may appear in a compact table.
Then show a large live chart.
CHART STYLE
Charts must match the monochrome system.
Use:

* black background
* extremely subtle gray grid
* thin lines
* different dash patterns
* different line weights
* different luminance levels
* direct line labels when practical

Do not rely on color.
Example:
Primary — solid bright line
Replica 01 — thinner gray line
Replica 02 — dashed gray line
Use precise square or stepped interpolation for counters when appropriate.
DO NOT smooth discrete LSN data into decorative curves.
No gradients beneath charts.
No filled area charts unless there is a genuine quantitative reason.
No glowing chart lines.
No unnecessary legends if labels can sit directly beside lines.
Crosshair:
thin gray lines
Tooltip:
small square-cornered black technical tooltip with monospace data
REPLICATION DETAILS
Display:
PRIMARY DURABLE LSN
184229
REPLICA-01
APPLIED 184229
LAG 0
REPLICA-02
APPLIED 184184
LAG 45
CONNECTED
2 / 2
If synchronous replication is enabled, additionally show:
SYNCHRONOUS ACKS
1 REQUIRED
ACK TIMEOUTS
0
Do not show this block if sync replication is disabled.
Conditional interface is preferable to inactive clutter.
THROUGHPUT
Compute operations per second from differences in cumulative counters between polls.
Show:
reads/s
writes/s
total ops/s
Use a small trend chart.
Do not fabricate historical data while waiting for the first samples.
Before sufficient samples exist, explicitly display:
COLLECTING SAMPLES
LATENCY
Show:
READ
p50
p95
p99
WRITE
p50
p95
p99
Use one compact matrix.
Example:

```
          P50        P95        P99
```

READ 0.42ms 1.81ms 3.22ms
WRITE 1.17ms 4.12ms 6.82ms
Do not create six cards.
DURABILITY + WAL
This section should expose what makes DistributeDB interesting.
Display:
current_lsn
durable_lsn
wal_entries
snapshot_lsn
wal_syncs_total
wal_sync_time_avg
wal_sync_time_max
wal_sync_errors
Build a custom horizontal WAL visualization.
Concept:
SNAPSHOT DURABLE CURRENT
│ │ │
────┼────────────────────────────┼─────────────┼────
175000 184229 184241
The region before `durable_lsn` represents acknowledged durable state.
The small region between `durable_lsn` and `current_lsn` communicates writes awaiting disk durability during group commit.
Do NOT make this look like a generic progress bar.
Make it look like a log-position instrument.
WAL ERROR STATE
If WAL sync errors > 0:
do not turn the interface red.
Instead show a high-contrast monochrome warning:
┌ WAL SYNC ERROR ──────────────────┐
│ wal_sync_errors_total 3 │
└──────────────────────────────────┘
Potentially use:

* brighter border
* inversion
* warning glyph
* heavier typography

Monochrome only.
RECOVERY
Show recovery as a process.
LAST RECOVERY
recovery time
records replayed
snapshot position
recovered position
Visual:
SNAPSHOT
175000
│
└──────── WAL REPLAY ──────────── CURRENT
214 RECORDS 184229
This should allow a reviewer to immediately understand why snapshots matter.
LSM STORAGE
Only show this section if:
storage engine == LSM
Do not show an empty disabled panel otherwise.
Metrics:
memtable size
table count
level-0 tables
bytes on disk
flush count
compaction count
If useful, create a minimal structural diagram:
MEMTABLE
│
│ flush
↓
L0 SSTABLES
│
│ compaction
↓
SSTABLE STORAGE
Use thin lines and typography.
No colorful database-cylinder icons.
No generic storage illustrations.
LOCK CONTENTION
Show:
read-lock wait percentiles
write-lock hold percentiles
Use a compact technical table and optionally a small temporal plot.
This section is for performance debugging.
Do not decorate it.
KEY EXPLORER
The Key Explorer is read-only.
Two modes:
GET / EXISTS
and
SCAN
GET
Layout:
KEY
[ user:10029________________________ ] [ GET ]
RESULT
user:10029
EXISTS
true
SIZE
38 B
VALUE
{"name":"Aaron","role":"admin"}
[ UTF-8 | HEX ]
Do not turn UTF-8 / HEX into giant pill buttons.
Use a minimal segmented text toggle.
HEX VIEW
Use a real technical hex viewer.
Example:
00000000 7B 22 6E 61 6D 65 22 3A 22 41 61 72 6F 6E 22 7D
If helpful, include ASCII representation on the right.
Use monospace exclusively.
SCAN
Fields:
START KEY
END KEY
PAGE SIZE
Results:
KEY
VALUE PREVIEW
SIZE
Dense rows.
Approximately 28–34px row height.
Do not create a card for every key.
Provide pagination controls.
WRITE CONSOLE
The write console must be visually and conceptually separated from read-only exploration.
Header:
WRITE CONSOLE
Subtext:
LOCAL DEVELOPMENT INTERFACE
No dramatic danger colors.
Available commands:
SET
DELETE
Provide compact controls.
TRANSACTIONS
Support:
BEGIN
SET
DELETE
COMMIT
ROLLBACK
When transaction active:
TRANSACTION / ACTIVE
01 SET account 100
02 SET account 200
03 DELETE temp
ROLLBACK COMMIT 3 OPS
Use a queue/log presentation.
Do not make each operation a card.
After successful commit:
COMMITTED
operations 3
LSN 184242–184244
durability acknowledged
sync 2.4 ms
if these values are available.
Never invent unavailable values.
FAULT-TOLERANCE DEMO
Create a final section:
FAULT-TOLERANCE LAB
Do not call it:
Demo Experience
Interactive Journey
Guided Experience
This is an engineering experiment.
Steps:
01
SEED DATABASE
02
STOP REPLICA-02
03
CONTINUE WRITES
04
RESTART REPLICA-02
05
OBSERVE CATCH-UP
States:
PENDING
RUNNING
COMPLETE
Again, no green checkmarks.
Use typography and luminance.
Completed:
bright text
Pending:
muted text
Current:
outlined row or brighter border
FAULT DEMO BEHAVIOR
When replica-02 stops:

* its node changes to DISCONNECTED
* topology connector changes to interrupted/dashed
* its applied_lsn stops moving
* replicas_connected decreases
* its chart line becomes stationary

As writes continue:

* primary durable_lsn continues increasing
* replica lag increases

When restarted:

* connection returns
* applied_lsn begins advancing
* replica catches up
* lag approaches zero
* status becomes SYNCED

The visual satisfaction must come from REAL METRICS changing.
Do not add fake celebration animation.
DATA ARCHITECTURE
Keep the dashboard outside the database process.
Architecture:
DistributeDB primary + replicas
│
│ existing client protocol
↓
ddb_dashboard
│
│ HTTP / JSON
↓
static dashboard
The database itself must not gain an HTTP server.
DDB_DASHBOARD
Create a small Rust std-only bridge.
Responsibilities:

* knows node addresses from the cluster launcher
* polls STATS approximately once per second
* stores a short rolling history in memory
* serves frontend static assets
* serves current cluster state
* serves metric history
* proxies GET
* proxies EXISTS
* proxies SCAN
* exposes deliberate local-development endpoints for SET/DELETE transactions if Write Console enabled
* exposes start/stop node actions only for the Fault-Tolerance Lab when the cluster launcher supports them

Do not build:
authentication
accounts
permissions
RBAC
failover
leader election controls
cluster reconfiguration
production management controls
They are outside project scope.
CLUSTER LAUNCHER
The local cluster launcher should know:
primary process
replica processes
ports
addresses
PIDs/process handles
storage mode
durability configuration
The dashboard may use it to:
start cluster
stop individual replica
restart replica
Do not expose arbitrary shell command execution.
FRONTEND TECHNOLOGY
The frontend should remain intentionally simple.
Prefer:
HTML
CSS
vanilla JavaScript
SVG
Canvas
Three.js only for the signature topology animation
Do not introduce:
React
Next.js
Vue
Svelte
Tailwind
Material UI
shadcn
Bootstrap
large chart frameworks
unless the existing repository already requires them.
This site should require no JavaScript build pipeline if practical.
The application must work on:
Windows
Linux
macOS
RESPONSIVENESS
Optimize for a developer using:
1440px desktop
1920px desktop
13–16 inch laptop
Do not compromise desktop density to create a mobile-first consumer UI.
At smaller sizes:

* charts stack
* metric strips wrap intelligently
* cluster topology converts to a vertical hierarchy
* tables become horizontally scrollable where required

Do not transform every component into giant mobile cards.
INTERACTIONS
Allowed:
subtle row hover
precise tooltips
small button-state transitions
chart crosshairs
slow database visualization motion
real-time metric changes
subtle opacity transition when node connectivity changes
Not allowed:
bounce
spring physics
card lift
tilt
parallax sections
animated gradients
magnetic buttons
cursor trails
text scramble
scroll-jacking
section reveal animation on every component
numbers repeatedly counting up from zero
STATES
Implement real states for:
LOADING
NO DATA
NODE UNAVAILABLE
BRIDGE UNAVAILABLE
DISCONNECTED
EMPTY DATABASE
NO SCAN RESULTS
LSM DISABLED
SYNC REPLICATION DISABLED
WRITE FAILURE
MALFORMED RESPONSE
The distinction between:
"still loading"
and:
"system unavailable"
must always be clear.
Do not display endless skeletons for offline systems.
COPY STYLE
Use terse systems language.
Good:
REPLICATION
Primary durable position vs replica applied positions.
Good:
LAST RECOVERY
214 records replayed from snapshot LSN 175000.
Bad:
Stay on top of your replication performance with powerful real-time insights.
Bad:
Experience unparalleled visibility into your distributed database.
Never write marketing copy.
Never use:
powerful
seamless
revolutionary
next-generation
cutting-edge
supercharge
unlock
intelligent insights
This is a technical instrument.
ICONS
Use almost no icons.
Prefer typography and simple geometric marks.
Do not automatically put icons beside:
Overview
Replication
Storage
Explorer
Demo
The labels are sufficient.
If an icon is needed for a technical action, use a minimal monochrome line icon.
COMPONENT RULE
Before creating a new UI component, ask:
Does this component expose or control actual DistributeDB functionality?
If no:
do not create it.
Before creating another card, ask:
Can this information instead be represented using:

* alignment
* table rows
* separators
* typography
* a diagram
* whitespace

If yes:
do that instead.
NO INVENTION RULE
Never invent:

* metrics
* APIs
* commands
* settings
* database features
* cluster features
* node states
* benchmark values
* sample historical values

When backend data is not connected, use clearly labeled development fixtures in code.
Do not make fake data appear as real database state.
INFORMATION DENSITY
Aim for the density of serious engineering software.
The user should be able to inspect several related measurements without scrolling through one metric per screen.
Prefer:
one well-designed panel containing eight logically related values
over:
eight visual cards.
VISUAL DETAIL
Use small technical details sparingly:

* 1px rules
* compact monospace metadata
* subtle column guides
* aligned numeric values
* tabular numerals
* tiny timestamps
* short metric descriptions
* structured whitespace

Do not cover the interface in decorative grid lines.
ANTI-AI-SLOP REVIEW
Before considering the frontend complete, perform an explicit design review.
Remove anything that appears to have been added simply because modern websites often contain it.
Check every section.
Ask:
Did I add a card that was not requested?
Did I add a feature that was not requested?
Did I invent a metric?
Did I add a colored status dot?
Did I add an unnecessary icon?
Did I add unnecessary rounded corners?
Did I use a gradient?
Did I add glow?
Did I add a decorative animation?
Did I use marketing copy?
Did I create empty space just to make the interface look luxurious?
Did I make technical data less dense than necessary?
Did I make the interface look like a SaaS template?
Did I hide machine terminology behind consumer-friendly labels?
Did I add UI because it looked impressive rather than because it communicates system behavior?
If yes to any of these, remove or simplify it.
FINAL EXPERIENCE
When someone opens DistributeDB, they should immediately understand:
There is one primary.
There are replicas.
Writes pass through an ordered durable log.
The primary has a current and durable log position.
Replicas apply those entries independently.
Replication can lag.
A replica can disappear and later catch up.
Snapshots affect recovery.
The LSM tree has observable internal behavior.
The database exposes enough instrumentation to empirically study those mechanisms.
The frontend exists to make those mechanics visible.
The intended emotional response is not:
"cool futuristic website."
It is:
"this person actually built and understands a database."
Build exactly that.
