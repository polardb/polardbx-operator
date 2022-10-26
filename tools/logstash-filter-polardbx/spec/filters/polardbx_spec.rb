# encoding: utf-8

require 'spec_helper'
require 'logstash/filters/polardbx'

describe LogStash::Filters::Polardbx do

  describe 'cn sql log testcase 1' do

    let(:event) do
      LogStash::Event.new({ 'fields' => {}, 'message' => {} }).tap { |obj|
        obj.set('[fields][log_type]', 'cn_sql_log')
      }
    end

    let(:subject) do
      LogStash::Filters::Polardbx.new({}).tap { |filter| filter.register }
    end

    it "sql log 1" do
      message = '2021-03-15 16:18:47.774 - [user=root,host=100.117.245.37,port=56191,schema=tpcc100]  [TDDL] [autocommit=0,TSO] [V3] [len=59] UPDATE bmsql_warehouse SET w_ytd = w_ytd + ? WHERE w_id = ? [len=11] [741.02,29] # [rt=66,rows=1,type=001,mr=0,bid=-1,pid=-1,wt=TP,lcpu=1,lmem=0,lio=0,lnet=0.0] # 1227ae9f50c04000-3, tddl version: 5.4.9-16155600'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 2" do
      message = '2021-03-15 16:18:47.784 - [user=root,host=100.117.246.37,port=25155,schema=tpcc100]  [TDDL] [prepare] [stmt_id:3251] [autocommit=0,null] [V3] [len=306] INSERT INTO bmsql_order_line ( ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?) [len=2] [] # [rt=0,rows=0,bid=-1,pid=-1,wt=TP,lcpu=0,lmem=0,lio=0,lnet=0] # 1227ae9f53404000-14, tddl version: 5.4.9-16155600'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 3" do
      message = '2021-03-15 16:18:47.785 - [user=root,host=100.117.246.37,port=25155,schema=tpcc100]  [TDDL] [autocommit=0,TSO] [V3] [len=306] INSERT INTO bmsql_order_line ( ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?) [len=346] [18778,5,16,1,2247,16,1,68.44,"wt5NwFK8dIOfQClV0vkxZBLX",18778,5,16,2,12007,16,9,163.08,"GCZQx9jW3LfPVyxiffCegbCf",18778,5,16,3,22406,16,4,396.36,"y86rfNZNXJzoj0T4LvKAB4CV",18778,5,16,4,36765,16,9,513.99,"pgF4vOa0sx9uaKKBvXzCrL59",18778,5,16,5,59172,16,3,208.62,"f3eh9XAsSXnnx7JQZnKjuArW",18778,5,16,6,87527,16,1,42.39,"rqJWjIUKmMMnyWPRsl2UQqgC"] # [rt=0,rows=6,type=001,mr=0,bid=-1,pid=-1,wt=TP,lcpu=1,lmem=0,lio=0,lnet=0.0] # 1227ae9f53404000-15, tddl version: 5.4.9-16155600'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 4" do
      message = '2021-03-15 16:18:47.785 - [user=root,host=100.117.246.37,port=25155,schema=tpcc100]  [TDDL] [autocommit=0,TSO] [V3] [len=306] INSERT INTO bmsql_order_line ( ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?) [len=346] [18778,5,16,1,2247,16,1,68.44,"wt5NwFK8dIOfQClV0vkxZBLX",18778,5,16,2,12007,16,9,163.08,"GCZQx9jW3LfPVyxiffCegbCf",18778,5,16,3,22406,16,4,396.36,"y86rfNZNXJzoj0T4LvKAB4CV",18778,5,16,4,36765,16,9,513.99,"pgF4vOa0sx9uaKKBvXzCrL59",18778,5,16,5,59172,16,3,208.62,"f3eh9XAsSXnnx7JQZnKjuArW",18778,5,16,6,87527,16,1,42.39,"rqJWjIUKmMMnyWPRsl2UQqgC"] # [rt=0,rows=6,type=001,mr=0,bid=-1,pid=-1,wt=TP,lcpu=1,lmem=0,lio=0,lnet=0.0] # 1227ae9f53404000-15, tddl version: 5.4.9-16155600'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 5" do
      message = '2021-04-10 20:32:07.916 - [user=polardbx_root,host=127.0.0.1,port=51786,schema=polardbx]  [TDDL] [V3] [len=22] drop database busutest [len=2] [] # [rt=92,rows=0,type=000,mr=0,bid=-1,pid=-1,wt=TP,lcpu=1,lmem=0,lio=0,lnet=0.0] # 124961f12d803000, tddl version: 5.4.9-16153688'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 6" do
      message = '2021-03-15 16:18:47.786 - [user=root,host=100.117.246.37,port=25155,schema=tpcc100]  [TDDL] [autocommit=0,null] [V3] [len=8] rollback [len=2] [] # [rt=0,rows=0,bid=-1,pid=-1,wt=TP,lcpu=0,lmem=0,lio=0,lnet=0] # 1227ae9f64c04000-1, tddl version: 5.4.9-16155600'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 7" do
      message = '2021-03-15 16:18:47.786 - [user=root,host=100.117.246.37,port=25155,schema=tpcc100]  [TDDL] [autocommit=0,null] [V3] [len=8] rollback [len=2] [] # [rt=0,rows=0,bid=-1,pid=-1,wt=TP,lcpu=0,lmem=0,lio=0,lnet=0] # 1227ae9f64c04000-1, tddl version: 5.4.9-16155600'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 8" do
      message = '2021-03-15 16:18:47.786 - [user=root,host=100.117.246.37,port=25155,schema=tpcc100]  [TDDL] [autocommit=0,null] [V3] [TOO LONG] [len=8] rollback [len=2] [] # [rt=0,rows=0,bid=-1,pid=-1,wt=TP,lcpu=0,lmem=0,lio=0,lnet=0] # 1227ae9f64c04000-1, tddl version: 5.4.9-16155600'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

    it "sql log 9" do
      message = "2021-06-15 17:42:56.546 - [user=root,host=172.16.0.1,port=44262,schema=testdb]  [TDDL] [V3] [len=76] insert into PRODUCTS values(3,'产品名称33',3,'130',210),(4,'产品名称44',1,'110',160) [len=2] [] # [rt=11,rows=2,ts=1649757251592,type=000,frows=0,arows=2,scnt=1,mem=-1,mpct=-1.0,smem=-1,tmem=-1,ltc=3303866,lotc=2470007,letc=833859,ptc=7570307,pstc=7570307,prstc=-1,pctc=4759,sct=0,mbt=0,tid=1fdfeca3,ts=1623750176534,mr=0,bid=-1,pid=-1,wt=TP,em=CURSOR,lcpu=2,lmem=0,lio=0,lnet=1.0] # 129e3425cfc02000, tddl version: 5.4.10-16231545"
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
    end

  end

  describe 'cn slow log testcase 1' do

    let(:event) do
      LogStash::Event.new({ 'fields' => {}, 'message' => {} }).tap { |obj|
        obj.set('[fields][log_type]', 'cn_slow_log')
      }
    end

    let(:subject) do
      LogStash::Filters::Polardbx.new({}).tap { |filter| filter.register }
    end

    it "slow log 1" do
      message = '2021-03-25 09:37:05.846 - [user=root,host=100.117.245.209,port=39607,schema=busu]  [TDDL] SELECT i_price, i_name, i_data FROM bmsql_item WHERE i_id = 47845#4548#-1#1234328fe3c04001-10, tddl version: 5.4.9-SNAPSHOT'
      event.set('message', message)
      subject.filter(event)
      puts event.get('message').to_s
      puts "slow log 1 timestamp busu #{event.get('timestamp').to_s}"
    end

  end

end
