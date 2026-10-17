Adult meshes from OpnTec/bodyapps-viz / Fashiontec (2014), LGPL-3.0.
https://github.com/OpnTec/bodyapps-viz
Source revision: 45b7520c764d9549ac3382e7c08e2e240dd51842

male.json derives from models/skinned/UCS/basis.js; female.json from female.js.
Modifications: removed unused morph targets and skinning/animation fields;
retained Height, Waist and Hip Girth morphs; embedded their calibration ranges
from testconfig.json/femaleconfig.json. JSON is editable source, not compiled.
Original complete source and history are available at the revision above.
To replace models, retain the Three.js JSON v3 geometry format and
measurementRanges mapping (base, minimum, maximum) in cm.

Three.js r67, MIT licence, is provided separately as three-r67.js.
See LICENSE-three.txt, LICENSE-LGPL-3.0.txt and LICENSE-GPL-3.0.txt.

The renderer is illustrative, not a validated anthropometric reconstruction.
Missing dimensions use the template defaults; values beyond calibration are
clamped for rendering only. Weight and bra band size do not alter the shape.
The mixed group uses a generic adult male template, without inferring anatomy.

Insights adds procedural scalp hair, with loose shoulder-length hair for the
female figure, using the modal recorded hair colour. No hairstyle or length is inferred. Missing, tied, unknown or bald
values omit hair. Hair geometry is disposed together with the body on updates.
