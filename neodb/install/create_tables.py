'''
Created on Feb 2, 2015

@author: sergio
'''

#TODO create dedicated schema
import neodb.config as config

dbconn = config.dbconnect()
cursor = dbconn.cursor()


sql = """CREATE TABLE
            project
            (
                id INTEGER DEFAULT nextval('project_id_seq'::regclass) NOT NULL,
                INDEX INTEGER,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                DATE TIMESTAMP(6) WITHOUT TIME ZONE NOT NULL,
                PRIMARY KEY (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            block
            (
                id INTEGER DEFAULT nextval('block_id_seq'::regclass) NOT NULL,
                id_individual INTEGER,
                id_project INTEGER,
                file_datetime TIMESTAMP(6) WITHOUT TIME ZONE,
                rec_datetime TIMESTAMP(6) WITHOUT TIME ZONE,
                INDEX INTEGER,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT block_id_individual_fkey FOREIGN KEY (id_individual) REFERENCES individual (id),
                CONSTRAINT block_id_project_fkey FOREIGN KEY (id_project) REFERENCES project (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            recordingchannelgroup
            (
                id INTEGER DEFAULT nextval('recordingchannelgroup_id_seq'::regclass) NOT NULL,
                id_block INTEGER,
                channel_indexes BYTEA,
                channel_names BYTEA,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT recordingchannelgroup_id_block_fkey FOREIGN KEY (id_block) REFERENCES block (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            recordingchannel
            (
                id INTEGER DEFAULT nextval('recordingchannel_id_seq'::regclass) NOT NULL,
                INDEX INTEGER,
                coordinate BYTEA,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                id_recordingchannelgroup INTEGER,
                id_block INTEGER,
                PRIMARY KEY (id),
                CONSTRAINT recordingchannel_fk1 FOREIGN KEY (id_recordingchannelgroup) REFERENCES
                recordingchannelgroup (id),
                CONSTRAINT recordingchannel_fk2 FOREIGN KEY (id_block) REFERENCES block (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            segment
            (
                id INTEGER DEFAULT nextval('segment_id_seq'::regclass) NOT NULL,
                id_block INTEGER,
                file_datetime TIMESTAMP(6) WITHOUT TIME ZONE,
                rec_datetime TIMESTAMP(6) WITHOUT TIME ZONE,
                INDEX INTEGER,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT segment_id_block_fkey FOREIGN KEY (id_block) REFERENCES block (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            unit
            (
                id INTEGER DEFAULT nextval('unit_id_seq'::regclass) NOT NULL,
                id_recordingchannelgroup INTEGER,
                channel_indexes BYTEA,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT unit_id_recordingchannelgroup_fkey FOREIGN KEY (id_recordingchannelgroup)
                REFERENCES recordingchannelgroup (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            rcgroup_rc
            (
                id INTEGER DEFAULT nextval('rcgroup_rc_id_seq'::regclass) NOT NULL,
                id_recordingchannelgroup INTEGER,
                id_recordingchannel INTEGER,
                PRIMARY KEY (id),
                CONSTRAINT rcgroup_rc_id_recordingchannelgroup_fkey FOREIGN KEY (id_recordingchannelgroup)
                REFERENCES recordingchannelgroup (id),
                CONSTRAINT rcgroup_rc_id_recordingchannel_fkey FOREIGN KEY (id_recordingchannel) REFERENCES
                recordingchannel (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            spike
            (
                id INTEGER DEFAULT nextval('spike_id_seq'::regclass) NOT NULL,
                id_unit INTEGER,
                id_segment INTEGER,
                TIME DOUBLE PRECISION,
                waveform BYTEA,
                left_sweep DOUBLE PRECISION,
                sampling_rate DOUBLE PRECISION,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                INDEX INTEGER,
                id_recordingchannel INTEGER,
                p1 DOUBLE PRECISION,
                p2 DOUBLE PRECISION,
                p3 DOUBLE PRECISION,
                PRIMARY KEY (id),
                CONSTRAINT spike_id_unit_fkey FOREIGN KEY (id_unit) REFERENCES unit (id),
                CONSTRAINT spike_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES segment (id),
                CONSTRAINT spike_id_rc_fkey FOREIGN KEY (id_recordingchannel) REFERENCES recordingchannel
                (id)
            );"""
cursor.execute(query)


sql = """CREATE TABLE
    spiketrain
    (
        id INTEGER DEFAULT nextval('spiketrain_id_seq'::regclass) NOT NULL,
        id_unit INTEGER,
        id_segment INTEGER,
        times BYTEA,
        t_start DOUBLE PRECISION,
        t_stop DOUBLE PRECISION,
        waveforms BYTEA,
        left_sweep DOUBLE PRECISION,
        sampling_rate DOUBLE PRECISION,
        name CHARACTER VARYING(25),
        description CHARACTER VARYING(150),
        file_origin CHARACTER VARYING(50),
        PRIMARY KEY (id),
        CONSTRAINT spiketrain_id_unit_fkey FOREIGN KEY (id_unit) REFERENCES unit (id),
        CONSTRAINT spiketrain_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES segment (id)
    );"""
cursor.execute(sql)


sql = """CREATE TABLE
            irregularlysampledsignal
            (
                id INTEGER DEFAULT nextval('irregularlysampledsignal_id_seq'::regclass) NOT NULL,
                id_recordingchannel INTEGER,
                id_segment INTEGER,
                times BYTEA,
                VALUES BYTEA,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT irregularlysampledsignal_id_recordingchannel_fkey FOREIGN KEY
                (id_recordingchannel) REFERENCES recordingchannelgroup (id),
                CONSTRAINT irregularlysampledsignal_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES
                segment (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            cluster
            (
                id INTEGER DEFAULT nextval('cluster_id_seq'::regclass) NOT NULL,
                id_spike INTEGER NOT NULL,
                id_recordingchannel INTEGER NOT NULL,
                INDEX INTEGER NOT NULL,
                PRIMARY KEY (id),
                CONSTRAINT cluster_fk1 FOREIGN KEY (id_spike) REFERENCES spike (id),
                CONSTRAINT cluster_fk2 FOREIGN KEY (id_recordingchannel) REFERENCES recordingchannel (id)
            )"""
cursor.execute(query)


sql = """CREATE TABLE
            eventarray
            (
                id INTEGER DEFAULT nextval('eventarray_id_seq'::regclass) NOT NULL,
                id_segment INTEGER,
                times BYTEA,
                labels BYTEA,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT eventarray_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES segment (id)
            );"""
cursor.execute(query)


sql = """CREATE TABLE
            event
            (
                id INTEGER DEFAULT nextval('event_id_seq'::regclass) NOT NULL,
                id_segment INTEGER,
                TIME DOUBLE PRECISION,
                label CHARACTER VARYING(25),
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT event_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES segment (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            epocharray
            (
                id INTEGER DEFAULT nextval('epocharray_id_seq'::regclass) NOT NULL,
                id_segment INTEGER,
                times BYTEA,
                durations BYTEA,
                labels BYTEA,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT epocharray_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES segment (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            epoch
            (
                id INTEGER DEFAULT nextval('epoch_id_seq'::regclass) NOT NULL,
                id_segment INTEGER,
                TIME DOUBLE PRECISION,
                duration DOUBLE PRECISION,
                label CHARACTER VARYING(25),
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT epoch_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES segment (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            analogsignalarray
            (
                id INTEGER DEFAULT nextval('analogsignalarray_id_seq'::regclass) NOT NULL,
                id_segment INTEGER,
                id_recordingchannelgroup INTEGER,
                signal BYTEA,
                sampling_rate DOUBLE PRECISION,
                t_start DOUBLE PRECISION,
                channel_indexes BYTEA,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                file_origin CHARACTER VARYING(50),
                PRIMARY KEY (id),
                CONSTRAINT analogsignalarray_id_segment_fkey FOREIGN KEY (id_segment) REFERENCES segment
                (id),
                CONSTRAINT analogsignalarray_id_recordingchannelgroup_fkey FOREIGN KEY
                (id_recordingchannelgroup) REFERENCES recordingchannelgroup (id)
            );"""
cursor.execute(sql)


sql = """CREATE TABLE
            individual
            (
                id INTEGER DEFAULT nextval('individual_id_seq'::regclass) NOT NULL,
                INDEX INTEGER,
                name CHARACTER VARYING(25),
                description CHARACTER VARYING(150),
                picture OID,
                birth_date TIMESTAMP(6) WITHOUT TIME ZONE,
                PRIMARY KEY (id)
            );"""
cursor.execute(sql)

dbconn.commit()

if __name__ == '__main__':
    pass