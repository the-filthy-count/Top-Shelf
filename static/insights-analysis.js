/* Local aggregate analysis. National references never trigger remote API calls. */
(() => {
  const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
  const fmt=v=>v==null?'Not recorded':Number(v).toLocaleString(undefined,{maximumFractionDigits:3});
  const units={height:'cm',weight:'kg',waist:'cm',hips:'cm',band:'in',age:'years'};
  // CDC FastStats, NHANES Aug 2021–Aug 2023, adults 20+. Converted from inches / pounds.
  const refs={female:{height:63.5*2.54,weight:171.8*.45359237,waist:38.5*2.54},male:{height:68.9*2.54,weight:199*.45359237,waist:40.6*2.54}};
  const dialog=el('dialog',null,'insights-analysis');dialog.setAttribute('aria-labelledby','analysis-title');document.body.append(dialog);
  let opener;
  function close(){if(dialog.open)dialog.close();}
  window.closeInsightsAnalysis=close;
  dialog.addEventListener('close',()=>{if(dialog.open)return;if(opener?.isConnected)opener.focus();dialog.replaceChildren();});
  dialog.addEventListener('click',e=>{if(e.target===dialog){const r=dialog.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)close();}});
  function section(title){const s=el('section');s.append(el('h3',title));dialog.append(s);return s;}
  function table(root,headers,rows){const t=el('table'),head=el('thead'),tr=el('tr');headers.forEach(x=>tr.append(el('th',x)));head.append(tr);t.append(head);const b=el('tbody');rows.forEach(row=>{const r=el('tr');row.forEach(x=>r.append(el('td',x)));b.append(r);});t.append(b);root.append(t);}
  function bars(root,rows,denominator){const list=el('ol',null,'analysis-bars');const max=Math.max(1,...rows.map(r=>r.count));rows.forEach(row=>{const li=el('li');li.style.setProperty('--bar',row.count/max*100+'%');li.append(el('span',row.name),el('strong',fmt(row.count)+(denominator?' · '+(row.count/denominator*100).toFixed(1)+'%':'')));list.append(li);});root.append(list);}
  window.openInsightsAnalysis=({profile:p,group,field,kind,label,method})=>{
    if(dialog.open)close();opener=document.activeElement;dialog.replaceChildren();
    const header=el('header');const title=el('h2',label+' analysis');title.id='analysis-title';const button=el('button','×','analysis-close');button.type='button';button.setAttribute('aria-label','Close analysis');button.addEventListener('click',close);header.append(title,button);dialog.append(header);
    const v=p[kind]?.[field]||{},detail=p.analysis?.[kind]?.[field];
    dialog.append(el('p',`${group==='mix'?'Transgender / mixed':group} library group · ${fmt(v.samples||0)} of ${fmt(p.population)} recorded (${p.population?((v.samples||0)/p.population*100).toFixed(1):0}% coverage).`,'analysis-note'));
    if(!detail){dialog.append(el('p','Refresh Insights to load the expanded analysis.'));dialog.showModal();return;}
    if(!v.samples){dialog.append(el('p','No valid values recorded for this trait in this selection.'));dialog.showModal();return;}
    if(v.samples<20)dialog.append(el('p','Small recorded sample: treat these summaries as descriptive, not representative.','analysis-note'));
    if(field.includes('ratio'))dialog.append(el('p','Calculated for each performer with both measurements recorded, then summarised. This is not the ratio of two separate group averages.','analysis-note'));
    if(kind==='numeric'){
      const unit=units[field]||'',value=n=>fmt(n)+(n!=null&&unit?' '+unit:'');
      const s=section('Summary');const grid=el('dl',null,'analysis-summary');
      const mode=v.mode==null?(v.modes?.length?v.modes.map(value).join(' / '):'No repeated value'):value(v.mode);
      [['Mean',value(v.mean)],['Median',value(v.median)],['Mode',mode],['Standard deviation',value(detail.stddev)],['Minimum',value(detail.min)],['Maximum',value(detail.max)],['25th percentile',value(detail.q1)],['75th percentile',value(detail.q3)]].forEach(([k,x])=>{const d=el('div');d.append(el('dt',k),el('dd',x));grid.append(d);});s.append(grid);
      s.append(el('p','Standard deviation describes the recorded library population (divisor N), not uncertainty in the mean. Quartiles use linear interpolation. A single value has zero spread.','analysis-note'));
      const hist=section('Distribution');bars(hist,detail.histogram.map(b=>({name:value(b.low)+(b.high!==b.low?' – '+value(b.high):''),count:b.count})),v.samples);
      hist.append(el('p','Equal-width bins: lower boundary included, upper boundary excluded except for the last bin.','analysis-note'));
      const top=section('Five most frequent values');bars(top,detail.frequencies.map(r=>({...r,name:value(Number(r.name))})),v.samples);
      const extremes=section('Recorded extremes');const columns=el('div',null,'analysis-columns');for(const [key,title] of [['highest','Highest five'],['lowest','Lowest five']]){const col=el('div');col.append(el('h4',title));table(col,['Performer',label],detail[key].map(r=>[r.name,value(r.value)]));columns.append(col);}extremes.append(columns);
      if(['height','weight','waist'].includes(field)){
        const national=section('National reference');const select=el('select');select.setAttribute('aria-label','National comparison population');
        [['','Choose a reference'],['female','US women, age 20+'],['male','US men, age 20+']].forEach(([val,name])=>{const o=el('option',name);o.value=val;select.append(o);});select.value=group==='mix'?'':group;national.append(select);const result=el('div');national.append(result);
        const update=()=>{result.replaceChildren();if(!select.value){result.append(el('p','Select a published reference population; no national trans/mixed benchmark is supplied.'));return;}const ref=refs[select.value][field],delta=v.mean-ref;table(result,['Statistic','Value'],[['Library mean',value(v.mean)],['National mean',value(Math.round(ref*10)/10)],['Difference',`${delta>=0?'+':''}${value(Math.round(delta*10)/10)} (${(delta/ref*100).toFixed(1)}%)`]]);};select.addEventListener('change',update);update();
        const source=el('a','CDC / NCHS · NHANES August 2021–August 2023');source.href='https://www.cdc.gov/nchs/fastats/body-measurements.htm';source.target='_blank';source.rel='noopener noreferrer';national.append(source,el('p','US national reference, not an inferred country match. This is a descriptive mean-to-mean comparison: library ages and measurement methods differ, and the collection is not a representative population. No national standard deviation or significance test is inferred.','analysis-note'));
      }else section('National reference').append(el('p','No comparable national reference is included for this trait.'));
    }else{
      const counts=detail.slice(0,5),rest=detail.slice(5).reduce((s,r)=>s+r.count,0);if(rest)counts.push({name:'Other recorded values',count:rest});
      const chart=section('Composition');const layout=el('div',null,'analysis-columns');const pie=el('div',null,'analysis-pie');pie.setAttribute('role','img');pie.setAttribute('aria-label',counts.map(r=>`${r.name}: ${(r.count/v.samples*100).toFixed(1)}%`).join(', '));let cursor=0;const stops=counts.map((r,i)=>{const end=cursor+r.count/v.samples*100;const color=`color-mix(in srgb,var(--accent) ${100-i*13}%,var(--bg))`;const stop=`${color} ${cursor}% ${end}%`;cursor=end;return stop;});pie.style.background=`conic-gradient(${stops.join(',')})`;layout.append(pie);const legend=el('div');bars(legend,counts,v.samples);layout.append(legend);chart.append(layout);
      const all=section('Recorded values');table(all,['Value','Performers','Share of recorded'],detail.map(r=>[r.name,fmt(r.count),(r.count/v.samples*100).toFixed(1)+'%']));all.append(el('p','Missing values are excluded from percentages. Labels reflect saved metadata; equivalent provider terms may be recorded separately.','analysis-note'));
    }
    dialog.showModal();dialog.scrollTop=0;
  };
})();
