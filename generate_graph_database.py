import json
import os

def get_contributor_ids(item):
    contributors = item['contributors']
    artist_ids = [i['id'] for i in contributors if i['role'] == 'artist'] # TODO handle non-artist contributors
    artist_ids_str = ''
    for a_id in artist_ids:
        artist_ids_str += 'MATCH (a:Artwork), (b:Artist) WHERE a.id = {} AND b.id = {} CREATE (a)-[r:CREATED_BY]->(b);\n'.format(item['id'], a_id)
        # artist_ids_str += 'CREATE (artwork_{}:Artwork)-[r:CREATED_BY]->(artist_{}:Artist);\n'.format(item['id'], a_id)
    return artist_ids_str

# TODO: catalogueGroup, contributors, dateRange
artwork_fields = [
    'acno',
    'acquisitionYear', # TODO cast years to date
    'all_artists',
    'classification',
    'contributorCount',
    'creditLine',
    'dateText',
    'depth',
    'height',
    'id',
    'inscription',
    'medium',
    'movementCount',
    'subjectCount',
    'thumbnailCopyright',
    'thumbnailUrl',
    'title',
    'url',
    'width'
]

artwork_special_fields = {
    'contributors': get_contributor_ids
}

artist_special_fields = {
    # TODO birth and death details
}

artist_fields = [
    'birthYear', # TODO cast to date
    'date',
    'id',
    'fc',
    'gender',
    'id',
    'mda',
    'totalWorks',
    'url'
]

artwork_out_filename = 'create_artwork_db.txt'
artist_out_filename = 'create_artist_db.txt'
artist_artwork_out_filename = 'create_artist_artwork_edges.txt'
subjects_out_filename = 'create_subjects.txt'
subjects_edges_out_filename = 'create_subjects_edges.txt'
total_out_filename = 'create_entire_graphdb.txt'

all_subjects = {}

def get_files(start_dir, category):
    fnames = []
    for root, dirs, files in os.walk(start_dir):
        for name in files:
            if name.endswith('.json'):
                fnames.append(os.path.join(root, name))
    print('Total number of {} files:'.format(category), len(fnames))
    return fnames

def recurse_subjects(artwork_id, s, parent_id=None):
    s_name = s['name']
    s_id = s['id']
    s_children = s['children'] if 'children' in s else None
    if s_name != 'subject' and s_id not in all_subjects:
        s_metadata = {'name': s_name, 'parent': parent_id}
        if not s_children:
            s_metadata['artwork'] = artwork_id
        all_subjects[s_id] = s_metadata
    if s_children:
        for child in s_children:
            recurse_subjects(artwork_id, child, s_id)
    return

def process_category(out_fname, in_filenames, fields, special_fields, node_type):
    edges = ''
    with open(out_fname, 'w') as outfile:
        for n, fname in enumerate(in_filenames):
            with open(fname, 'r') as f:
                content = f.read()
                item = json.loads(content)
                item_id = item['id']
                item_str = 'CREATE ({}_{}:{} {{ '.format(node_type.lower(), item_id, node_type)
                for field in fields:
                    if field in item:
                        val = item[field]
                        if type(val) == str and '\'' in val:
                            val = val.replace('\'','"')
                        if type(val) == str and '`' in val:
                            val = val.replace('`', '"')
                        try:
                            val = int(val)
                            if 'Year' in field:
                                val = 'date(\'{}\')'.format(val)
                        except Exception:
                            val = '\'{}\''.format(val)
                        item_str += '{}: {}, '.format(field, val)
                for field in ['contributors']:
                    if field in item:
                        edges += special_fields[field](item)
                item_str = item_str[:-2]
                item_str += '});\n'
                outfile.write(item_str)
                if 'subjects' in item:
                    subjects = item['subjects']
                    recurse_subjects(item_id, subjects)
                if n % 1000 == 0 and n > 0:
                    print('Processed {} {} files'.format(n, node_type))
    if node_type == 'Artwork':
        with open(artist_artwork_out_filename, 'w') as artist_artwork_out:
            artist_artwork_out.write(edges)
        with open(subjects_out_filename, 'w') as subjects_out:
            with open(subjects_edges_out_filename, 'w') as subjects_edges_out:
                for s in all_subjects:
                    name = all_subjects[s]['name']
                    parent = all_subjects[s]['parent']
                    artwork_id = all_subjects[s]['artwork'] if 'artwork' in all_subjects[s] else None
                    artwork_str = ', artwork: {}'.format(artwork_id) if artwork_id else ''
                    parent_str = ', parent: {}'.format(parent) if parent else ''
                    l = 'CREATE (subject_{}:Subject {{ name: "{}"{}{}, id: {} }});\n'.format(s, name, parent_str, artwork_str, s)
                    subjects_out.write(l)
                    parent_edge = 'MATCH (p:Subject), (c:Subject) WHERE p.id = {} AND c.id = {} CREATE (p)-[r:SUBJ_PARENT_OF_SUBJ]->(c);\n'.format(parent, s)
                    subjects_edges_out.write(parent_edge)
                    if artwork_id:
                        artwork_edge = 'MATCH (s:Subject), (a:Artwork) WHERE s.id = {} AND a.id = {} CREATE (s)-[r:SUBJ_OF_ARTWORK]->(a);\n'.format(s, artwork_id)
                        subjects_edges_out.write(artwork_edge)
                    # parent_edge = 'CREATE (subject_{}:Subject)-[r:SUBJ_PARENT_OF_SUBJ]->(subject_{}:Subject);\n'.format(parent, s)
                    # artwork_edge = 'CREATE (subject_{}:Subject)-[r:SUBJ_OF_ARTWORK]->(artwork_{}:Artwork);\n'.format(s, artwork_id)

def main():
    artwork_in_filenames = ['./tate-collection/artworks/n/009/n00926-2946.json', './tate-collection/artworks/n/027/n02738-1578.json']
    artist_in_filenames = ['./tate-collection/artists/b/brown-sir-arnesby-821.json', './tate-collection/artists/c/crome-john-126.json']
    # artwork_in_filenames = get_files('./tate-collection/artworks/', 'Artwork')
    # artist_in_filenames = get_files('./tate-collection/artists/', 'Artist')

    process_category(artwork_out_filename, artwork_in_filenames, artwork_fields, artwork_special_fields, 'Artwork')
    process_category(artist_out_filename, artist_in_filenames, artist_fields, artist_special_fields, 'Artist')

    concat_filenames = [artwork_out_filename, artist_out_filename, subjects_out_filename, artist_artwork_out_filename, subjects_edges_out_filename]
    with open(total_out_filename, 'w') as total_out:
        for fname in concat_filenames:
            with open(fname, 'r') as infile:
                for line in infile:
                    total_out.write(line)
main()

