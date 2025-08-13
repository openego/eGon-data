# Description

This directory is called “overlays” because it stored content for oemetadata generation oemetadata. The overlays job is to layer human-authored bits (titles, context, licenses, field hints) on top of what we auto-extract from the DB—rather than being the single source of truth. That mental model is base metadata → overlay additional metadata. Overlay can override what is added in previous steps.

## Notes

Much of the functionality does not have to be implemented in egon-data as it is general purpose and should be added to omi. Currently we have a conflict with the python version.

Also omi provides functionality to add template metadata. In the example below you see an entry for the context. In most cases the Project will be the same for all datasets this is when templates come in handy as we can apply them to all datasets in one go. If you specify the context here then it will override the template entry for that specific dataset. This is why the overlay can also be viewed as individual metadata.

## Usage

For each dataset used in the pipeline there should be metadata created. If we can provide additional metadata which cant be auto inferred from data sources then we use this directory and add yaml files.

A reasonable "minimal" yaml file would be what you see below. Keep in mind that this minimal content is not what is required. Here minimal refers more to what would be at least useful to add.

Example for zensus_vg250.yaml

```yaml
name: "society.destatis_zensus_population_per_ha_inside_germany"
title: "DESTATIS – Zensus 2011 – Population per hectare (inside Germany)"
description: >
  National census in Germany in 2011 filtered to German borders and cells with population > 0.
language: ["de-DE", "en-EN"]
context:
  homepage: "https://ego-n.org/"
  documentation: "https://egon-data.readthedocs.io/en/latest/"
  sourceCode: "https://github.com/openego/eGon-data"
  contact: "https://ego-n.org/partners/"
spatial:
  extent: "Germany"
  resolution: "1 ha"
temporal:
  referenceDate: "2011-12-31"
sources:
  - title: "Statistisches Bundesamt (Destatis) – Ergebnisse des Zensus 2011 zum Download"
    path: "https://www.zensus2011.de/DE/Home/Aktuelles/DemografischeGrunddaten.html"
licenses:
  - name: "DL-DE-BY-2.0"
    title: "Datenlizenz Deutschland – Namensnennung – Version 2.0"
    path: "https://www.govdata.de/dl-de/by-2-0"
contributors:
  - title: "Guido Pleßmann"
    email: "http://github.com/gplssm"
    comment: "Imported data"
  - title: "Jonathan Amme"
    email: "http://github.com/nesnoj"
    comment: "Metadata extended"
```
