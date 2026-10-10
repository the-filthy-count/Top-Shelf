/* On-demand adult mannequin renderer; model credits in vendor/bodyapps/README.txt. */
(() => {
  const host = document.getElementById('body-view');
  const fallback = document.getElementById('composite-figure');
  let skinColour = null;
  let renderer, scene, camera, mesh, version = 0, stopped = false;
  const cache = new Map();
  const requests = new AbortController();
  // Artistic palette only: ethnicity does not determine an individual's skin tone.
  function illustrativeSkin(profile) {
    const values = profile.categorical?.ethnicity?.values || [];
    if (values.length !== 1) return null;
    const key = values[0].trim().toLowerCase().replace(/[_-]+/g, ' ');
    const palette = {
      white:'#e0b99b', caucasian:'#e0b99b',
      black:'#825337', african:'#825337', 'african american':'#825337',
      asian:'#cba47e', 'east asian':'#cba47e',
      'south asian':'#ad7a53', indian:'#ad7a53',
      latin:'#bf926d', latina:'#bf926d', latino:'#bf926d', hispanic:'#bf926d',
      'middle eastern':'#ba8b63', arab:'#ba8b63',
      'native american':'#b88760', 'pacific islander':'#ad7d56'
    };
    return palette[key] || null;
  }
  function addHair(body, profile, group) {
    const values = profile.categorical?.hair_color?.values || [];
    const key = values.length === 1 ? values[0].trim().toLowerCase().replace(/[_-]+/g, ' ') : '';
    const colours = {
      black:0x171310, brown:0x704020, brunette:0x704020,
      'dark brown':0x382218, 'light brown':0x96643b,
      blonde:0xc9a45e, blond:0xc9a45e, 'dirty blonde':0xa98953,
      red:0x994724, auburn:0x773923, ginger:0xb65f2e,
      grey:0x92908b, gray:0x92908b, silver:0xbdbdb9, white:0xe3ded3,
      blue:0x315e93, pink:0xb9648c, purple:0x70528e, green:0x42785a
    };
    if (!(key in colours)) return; // Missing, tied and bald entries get no invented hair colour.
    // r67 Lambert ambient defaults to white: tint it too so ambient light cannot wash out the hair.
    const hairMaterial = () => new THREE.MeshLambertMaterial({color:colours[key],ambient:colours[key],side:THREE.DoubleSide});
    const source = body.geometry, box = source.boundingBox;
    const height = box.max.y-box.min.y, top = box.max.y;
    const head = source.vertices.filter(v => v.y > top-height*.065);
    const minZ = Math.min(...head.map(v=>v.z)), maxZ = Math.max(...head.map(v=>v.z));
    const centreZ = (minZ+maxZ)/2;
    const hair = new THREE.Geometry();
    // Conform a generic short hairstyle to the morphed scalp, keeping the face clear.
    hair.vertices = source.vertices.map(v => new THREE.Vector3(
      v.x*1.07, v.y+height*.006, centreZ+(v.z-centreZ)*1.07));
    source.faces.forEach(face => {
      const points = [source.vertices[face.a], source.vertices[face.b], source.vertices[face.c]];
      const y = points.reduce((sum,v)=>sum+v.y,0)/3;
      const z = points.reduce((sum,v)=>sum+v.z,0)/3;
      const front = Math.max(0,Math.min(1,(z-minZ)/(maxZ-minZ)));
      const hairline = top-height*(.074-.046*front);
      if (y > hairline) hair.faces.push(new THREE.Face3(face.a,face.b,face.c));
    });
    hair.computeVertexNormals();
    body.add(new THREE.Mesh(hair,hairMaterial()));
    if (group === 'female') {
      // Long hair sweeps behind the neck and shoulders, leaving the chest clear.
      const lengths = new THREE.Geometry(), rows=24, columns=48;
      for(let row=0;row<=rows;row++) {
        const t=row/rows;
        for(let column=0;column<=columns;column++) {
          const sweep=Math.min(1,t*3);
          const opening=.95+.65*sweep;
          const angle=opening+(Math.PI*2-2*opening)*column/columns;
          const wave=Math.sin(t*Math.PI*3+angle*2)*.0025;
          const width=height*(.044+.014*Math.sin(t*Math.PI*.7)+wave);
          const taper=1-.10*Math.pow(t,5);
          const x=Math.sin(angle)*width*taper;
          const y=top-height*(.036+.205*t)+height*.006*t*Math.cos(angle*5);
          const z=centreZ+Math.cos(angle)*height*(.060+.006*t)-height*.035*sweep;
          lengths.vertices.push(new THREE.Vector3(x,y,z));
        }
      }
      for(let row=0;row<rows;row++)for(let column=0;column<columns;column++) {
        const a=row*(columns+1)+column,b=a+1,c=a+columns+1,d=c+1;
        lengths.faces.push(new THREE.Face3(a,c,b),new THREE.Face3(b,c,d));
      }
      lengths.computeVertexNormals();
      body.add(new THREE.Mesh(lengths,hairMaterial()));
    }
    host.setAttribute('aria-label',host.getAttribute('aria-label')+'; illustrative '+(group==='female'?'loose shoulder-length hair':'short hair')+' in the most common recorded colour: '+values[0]);
  }
  function disposeBody() {
    if (!mesh) return;
    mesh.traverse(part => {part.geometry?.dispose(); part.material?.dispose();});
  }
  function draw() {
    if (!renderer || !mesh || stopped) return;
    const rgb = getComputedStyle(host).getPropertyValue('--brand-accent-rgb').trim();
    mesh.material.color.setStyle(skinColour || `rgb(${rgb || '50,180,180'})`);
    renderer.render(scene, camera);
  }
  async function model(group) {
    const name = group === 'female' ? 'female' : 'male';
    if (!cache.has(name)) cache.set(name, fetch(`/static/vendor/bodyapps/${name}.json`, {signal: requests.signal}).then(r => {
      if (!r.ok) throw Error('Body model unavailable');
      return r.json();
    }).catch(e => {cache.delete(name); throw e;}));
    return cache.get(name);
  }
  window.renderInsightsBody = async (group, profile, method) => {
    const current = ++version;
    if (!group) {host.hidden = true; return;}
    try {
      const raw = await model(group);
      if (current !== version || stopped) return;
      skinColour = illustrativeSkin(profile);
      host.setAttribute('aria-label', 'Illustrative adult body; ' + (skinColour ? 'skin colour is an artistic approximation based on the most common recorded ethnicity, not a measured trait' : 'accent colour used because ethnicity is missing, tied or has no palette entry'));
      if (!renderer) {
        renderer = new THREE.WebGLRenderer({alpha:true, antialias:true});
        renderer.setSize(240, 390);
        renderer.setPixelRatio?.(Math.min(devicePixelRatio, 2));
        host.append(renderer.domElement);
        scene = new THREE.Scene();
        camera = new THREE.PerspectiveCamera(30, 240/390, .1, 1000);
        scene.add(new THREE.AmbientLight(0x777777));
        const light = new THREE.DirectionalLight(0xffffff, .85);
        light.position.set(-3, 5, 8); scene.add(light);
      }
      if (mesh) {scene.remove(mesh); disposeBody();}
      const geometry = new THREE.JSONLoader().parse(raw).geometry;
      // Bake only supported measurements into the mesh, using upstream morph calibration.
      Object.entries(raw.measurementRanges).forEach(([field, range], index) => {
        const value = profile.numeric?.[field]?.[method];
        if (!Number.isFinite(value)) return;
        const [base, low, high] = range;
        const influence = (Math.max(low, Math.min(high, value)) - base)/(high-low);
        const target = geometry.morphTargets[index].vertices;
        geometry.vertices.forEach((v, i) => {
          const original = raw.vertices.slice(i*3, i*3+3);
          v.x += (target[i].x-original[0])*influence;
          v.y += (target[i].y-original[1])*influence;
          v.z += (target[i].z-original[2])*influence;
        });
      });
      geometry.computeVertexNormals(); geometry.computeBoundingBox();
      const box = geometry.boundingBox;
      const height = box.max.y-box.min.y;
      mesh = new THREE.Mesh(geometry, new THREE.MeshLambertMaterial({color:0x77aaaa}));
      mesh.position.set(-(box.min.x+box.max.x)/2, -(box.min.y+box.max.y)/2, -(box.min.z+box.max.z)/2);
      addHair(mesh, profile, group);
      scene.add(mesh);
      camera.position.set(0, 0, height*2.05); camera.lookAt(new THREE.Vector3(0,0,0));
      host.hidden = false; fallback.hidden = true; draw();
    } catch (e) {
      if (current === version && !stopped) {host.hidden = true; fallback.hidden = false;}
    }
  };
  const themeObserver = new MutationObserver(draw);
  themeObserver.observe(document.documentElement, {attributes:true, attributeFilter:['style','data-theme']});
  window.addEventListener('pagehide', () => {stopped=true; requests.abort(); themeObserver.disconnect(); disposeBody(); renderer?.dispose?.();});
})();
