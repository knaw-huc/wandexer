import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import requests
import yaml
import argparse
from annorepo.client import AnnoRepoClient, ContainerAdapter
from elasticsearch import ApiError, Elasticsearch
from loguru import logger

from indexer.SearchResultItem import SearchResultItem
from .SearchResultAdapter import SearchResultAdapter

MAPPING_FILE = f"{os.path.dirname(__file__)}/mapping.json"
CONFIG_DEFAULT = f"{os.path.dirname(__file__)}/config.yml"


def reset_index(
        elastic: Elasticsearch, index_name: str, path: str | os.PathLike
) -> bool:
    if elastic.indices.exists(index=index_name):
        logger.trace("Deleting previously existing ES index {}", index_name)
        try:
            res = elastic.indices.delete(index=index_name)
            logger.success("Deleted ES index {}: {}", index_name, res)
        except ApiError as err:
            logger.critical(err)
            return False

    mapping_path = Path(path)
    logger.trace("Creating ES index {} using mapping file: {}", index_name, mapping_path)
    try:
        res = elastic.indices.create(
            index=index_name,
            body=mapping_path.read_text(encoding="utf-8")
        )
    except ApiError as err:
        logger.critical(err)
        return False

    logger.success("Created ES index {}: {}", index_name, res)
    return True


def store_document(
        elastic: Elasticsearch, index: str, doc_id: str, doc: dict[str, Any]
) -> bool:
    resp = elastic.index(index=index, id=doc_id, document=doc)
    logger.trace(resp)
    if resp["result"] == "created":
        logger.success("Indexed {}", doc_id)
        return True

    logger.critical("Indexing {} failed: {}", doc_id, resp)
    return False


def extract_artworks(container: ContainerAdapter, overlap_query: dict[str, Any]) -> dict[str, set[str]]:
    # fetch overlapping Rs[type=artwork] annotations
    query = overlap_query.copy()
    query.update({
        "body.type": "Entity",
        "body.tei:type": "artwork"
    })
    logger.trace("artworks query: {}", query)

    artworks = defaultdict(set)
    for anno in SearchResultAdapter(container, query).items():
        logger.trace("artwork_anno: {}", anno)
        refs = anno.path("body.tei:ref")
        for ref in refs if type(refs) is list else [refs]:
            if 'head' in ref:
                head = ref['head']
                for lang, text in head.items():
                    logger.trace(f"adding[{lang}]={text}")
                    artworks[lang].add(text)
            else:
                logger.warning(f"missing 'head' in {ref}")

    return artworks


def extract_persons(container: ContainerAdapter, overlap_query: dict[str, Any]) -> set[str]:
    # construct query to fetch overlapping person annotations
    query = overlap_query.copy()
    query.update({
        "body.type": "Entity",
        "body.tei:type": "person"
    })
    logger.trace("persons query: {}", query)

    persons = set()
    for anno in SearchResultAdapter(container, query).items():
        anno_id = anno.path("body.id")
        logger.trace("person_anno: {}", anno)
        refs = anno.path("body.tei:ref")
        for ref in refs if type(refs) is list else [refs]:
            name = extract_name(anno_id, ref)
            if not name:
                name = f'unknown: {ref}'
            persons.add(name)

    return persons


def extract_name(anno_id: str, ref: dict[str, Any]) -> str|None:
    if 'sortLabel' in ref:
        return ref['sortLabel']

    if 'displayLabel' in ref:
        logger.warning("Using 'displayLabel' in lieu of 'sortLabel' in {}", anno_id)
        return ref['displayLabel']

    logger.error("Missing 'sortLabel' and 'displayLabel' in {}", anno_id)
    return None


def contrive_date(anno: SearchResultItem) -> dict[Any, Any] | None:
    actual = anno.path("body.dateSent")
    not_before = anno.path("body.dateSentNotBefore")
    not_after = anno.path("body.dateSentNotAfter")

    date = {}
    if actual:
        date["gte"] = actual
        date["lte"] = actual
        if not_before:
            logger.warning("{}: has both actual date AND notBefore!", anno.path("body.id"))
        if not_after:
            logger.warning("{}: has both actual date AND notAfter!", anno.path("body.id"))
    else:
        if not_before:
            date["gte"] = not_before

        if not_after:
            date["lte"] = not_after

    return date if date else None


def index_views(
        container: ContainerAdapter,
        elastic: Elasticsearch,
        index: str,
        docs,
        modules: list[str],
        fields: dict[str, str],
        views: dict[str, str],
) -> bool:
    logger.trace("docs: {}", docs)
    for doc_def in docs:
        doc_type = doc_def["type"]
        constraints = doc_def["constraints"]
        query = {}
        for c in constraints:
            path = c["path"]
            constraint = c["values"] if type(c["values"]) == str else {":isIn" : c["values"]}
            query[path] = constraint

        logger.trace("query[{}]: {}", doc_type, query)
        top_tier_anno_search: SearchResultAdapter = SearchResultAdapter(container, query)

        for anno in top_tier_anno_search.items():
            logger.trace("anno: {}", anno)

            doc_id = anno.path("body.id")

            target = anno.first_target_with_selector("NormalText")
            selector = target["selector"]
            overlap_base_query = {
                ":overlapsWithTextAnchorRange": {
                    "source": target["source"],
                    "start": selector["start"],
                    "end": selector["end"],
                },
            }

            doc = {}

            if 'type' in modules:
                doc['type'] = doc_type

            if 'date' in modules:
                doc['dateSortable'] = "9999"
                doc['date'] = {"gte": "0001", "lte": "9999"}
                date = contrive_date(anno)
                if date:
                    logger.info("setting ES doc date to: {}", date)
                    doc['date'] = date
                    if 'gte' in date:
                        doc['dateSortable'] = date['gte']
                    elif 'lte' in date:
                        doc['dateSortable'] = date['lte']
                else:
                    logger.warning("{}: no dateSent, winging it to {}, [sortable: {}]",
                                   doc_id, doc['date'], doc['dateSortable'])

                logger.info("{}: date: {}, dateSort: {}", doc_id, doc['date'], doc['dateSortable'])

            if 'title' in modules:
                # store title by language
                title_by_lang = anno.path('body.titles')
                if title_by_lang:
                    for lang in title_by_lang.keys():
                        lang_key = f"title{lang.upper()}"
                        doc[lang_key] = title_by_lang[lang]

            if 'fields' in modules:
                # store generic fields by path in anno
                for es_field, path in fields.items():
                    v = anno.path(path)
                    if v:
                        doc[es_field] = v

            if 'artworks' in modules:
                # store artworks
                artworks = extract_artworks(container, overlap_base_query)
                logger.trace(" - artworks: {}", artworks)
                for lang in artworks.keys():
                    lang_key = f"artworks{lang.upper()}"
                    doc[lang_key] = sorted(artworks[lang])

            if 'persons' in modules:
                # store persons
                persons = extract_persons(container, overlap_base_query)
                logger.trace(" - persons: {}", persons)
                doc['persons'] = sorted(persons)

            if 'views' in modules:
                # store views
                for view in views:
                    view_name = f"{view["name"]}Text"

                    view_texts = None
                    if 'constraints' in view:
                        overlap_query = overlap_base_query.copy()
                        for constraint in view["constraints"]:
                            overlap_query[constraint["path"]] = {":isIn": constraint["values"]}
                        logger.trace(" - overlap query: {}", overlap_query)

                        overlap_search: SearchResultAdapter = SearchResultAdapter(container, overlap_query)

                        view_texts = []
                        for overlap_anno in overlap_search.items():
                            logger.trace(" - overlap_anno: {}", overlap_anno)
                            text_target = overlap_anno.first_target_without_selector("NormalText")

                            resp = requests.get(text_target["source"], timeout=5)
                            if resp.status_code != 200:
                                logger.warning("Failed to get text for {}: {}",
                                               overlap_anno.path("body.id"),
                                               resp)
                            else:
                                view_texts.append(resp.text)
                                logger.trace(f" - {view_name}={view_texts}")
                    else:
                        text_target = anno.first_target_without_selector('NormalText')
                        logger.trace("text_target: {}", text_target)
                        logger.trace("source: {}", text_target['source'])
                        resp = requests.get(text_target['source'], timeout=5)
                        if resp.status_code != 200:
                            logger.warning("Failed to get text for {}: {}", doc_id, resp)
                        else:
                            view_texts = resp.text
                            logger.trace(f" - {view_name}={view_texts}")

                    if view_texts:
                        doc[view_name] = view_texts
                    else:
                        logger.warning(f"Empty '{view["name"]}' view")

            logger.debug(" - es_doc[{}]: {}", doc_id, doc)
            if  not store_document(elastic, index, doc_id, doc):
                return False

    return True


def main(
        ar_host: str,
        ar_container: str,
        es_host: str,
        es_index: str,
        cfg_path: Optional[str] = None,
        show_progress: bool = False,
        log_file_path: Optional[str] = None,
) -> None:
    if not show_progress:
        logger.remove()
        logger.add(sys.stdout, level="WARNING")

    if log_file_path:
        logger.remove()
        if os.path.exists(log_file_path):
            os.remove(log_file_path)
        logger.add(log_file_path)

    path = CONFIG_DEFAULT if cfg_path is None else cfg_path
    with open(path, "r", encoding="utf-8") as file:
        conf = yaml.safe_load(file)

    print(f"Indexing {ar_host}/{ar_container} to {es_host}/{es_index}")
    annorepo = AnnoRepoClient(ar_host)
    container = annorepo.container_adapter(ar_container)
    logger.info("AnnoRepo: {about}", about=annorepo.get_about())

    elastic = Elasticsearch(es_host)
    logger.info("ElasticSearch: {info}", info=elastic.info())

    mapping_path = conf["mapping"] if 'mapping' in conf else MAPPING_FILE

    if reset_index(elastic, es_index, mapping_path):
        index_views(
            container,
            elastic,
            es_index,
            docs=conf["docs"],
            modules=conf["modules"],
            fields=conf["fields"],
            views=conf["views"],
        )


def cli():
    parser = argparse.ArgumentParser(
        description="index annorepo container to elastic index"
    )
    parser.add_argument(
        "--annorepo-host",
        required=True,
        help="the AnnoRepo host name"
    )
    parser.add_argument(
        "--annorepo-container",
        required=True,
        help="the AnnoRepo container on annorepo-host",
    )
    parser.add_argument(
        "--elastic-host",
        required=False,
        default="http://localhost:9200",
        help="the ElasticSearch host name",
    )
    parser.add_argument(
        "--elastic-index",
        required=True,
        help="the ElasticSearch index name",
    )
    parser.add_argument(
        "--config",
        required=False,
        default=CONFIG_DEFAULT,
        help="configuration file",
    )
    parser.add_argument(
        "--trace",
        required=False,
        action="store_true",
        help="run indexer with logging in trace mode",
    )
    parser.add_argument(
        "--progress",
        required=False,
        action="store_true",
        help="Be more verbose",  # rename to '--verbose' ?
    )
    args = parser.parse_args()

    if args.trace:
        logger.remove()
        logger.add(sys.stderr, level="TRACE")
        logger.trace("TRACE ENABLED")

    main(
        args.annorepo_host,
        args.annorepo_container,
        args.elastic_host,
        args.elastic_index,
        args.config,
        args.progress
    )

if __name__ == "__main__":
    cli()
