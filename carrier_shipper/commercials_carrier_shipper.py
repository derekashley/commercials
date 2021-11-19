import pandas as pd
import xlrd
import numpy as np
#import json as j
import json
import pprint as pp
import urllib, urllib.request as url
from flask import Flask, jsonify, request
import requests
from numpy import nan
from flask_cors import CORS
import csv
import psycopg2
import logging
import datetime
from num2words import num2words
from decimal import Decimal

JSONDataErrorFlag = "False"
JSONDataErrorMsg = "Failed"

app = Flask(__name__)
CORS(app)
#app.debug = True
app.config["DEBUG"] = True

logging.basicConfig(filename='/root/python_files/log_file_demo2.log',level=logging.INFO,format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

@app.route('/whatsapp_notification', methods = ['POST'])

def whatsapp_notification():
	try:
		content = request.get_json()
		message=content["message"]
		number=content["number"]

		multipart_data = MultipartEncoder(
		fields={
				'message': message, 
				'key': 'ft1dcT8j16fIOknh',
				'number': number
				}
		)

		response = requests.post('http://send.wabapi.com/text.php', data=multipart_data,
						headers={'Content-Type': multipart_data.content_type})
	except Exception as e:
		print(e)
	return jsonify(response.json())

@app.route('/ftl_bulkbooking',methods=['GET','POST'])

def ftl_bulkbooking():
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()	
		print('Hi')

		f = request.files["uploadfile"]
		content = request.form
		#print(a['branch_name'])
		#content = json.loads(a)
		
		branch = content['branch_name']
		print(branch)
		customer = content['customer_name']
		vendor = content['vendor_name']
		warehouse = content['warehouse_name']

		branch_id = content['branch_id']
		customer_id  = content['customer_id']
		carrier_id  = content['carrier_id']
		warehouse_id  = content['warehouse_id']
		pickup_address_id  = content['pickup_address_id']
		pickup_address = content['pickup_address']

		customer_from_state  = content['customer_from_state']
		customer_from_addressname  = content['customer_from_addressname']
		customer_from_city  = content['customer_from_city']

		df = pd.read_excel(f.stream,engine = 'openpyxl')
		#df.dropna(how = 'all',inplace = True)
		df["material"] = df["Material Type*"]
		df["weight"] = df["Actual Weight*"]
		validation_columns = ["Destination/Consignee*","State*","City*","Location*","LR Number","Vendor LR Number","Material Type*","Material Description","No.Of Packages*","length","breadth","height","weighttype","Chargeable Weight","Actual Weight*","Invoice Number","Invoice Value","Invoice Date","Ewaybill No","Valid From","Valid To","gst_number"
		]
		
		check =  all(item in df.columns for item in validation_columns)
		print(check)
		if check is False:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
		
		for i in range(0,len(validation_columns)):
			validation_columns[i] = validation_columns[i].strip()
			df.rename(columns = {validation_columns[i]:validation_columns[i].replace(" ","_").lower()}, inplace = True)
		
		df.rename(columns = {"no.of_packages*":"no_of_items"}, inplace = True)
		print(df.columns)
		df_approved = df[0:0]
		df_disapproved = df[0:0]
		df_disapproved["Reason"] = ""
		df_drops = df
		if len(df_drops) <= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Uploaded excel sheet is empty." , "rejectdata":[],"ids":[] }'))

		df_drops['branch'] = branch
		df_drops['customer'] = customer
		#df_drops['vendor'] = vendor
		df_drops['warehouse'] = warehouse
		#df_drops['pickup_address'] = pickup_address

		df_drops['branch_id'] = branch_id
		df_drops['customer_id'] = customer_id
		df_drops['carrier_id'] = carrier_id
		df_drops['warehouse_id'] = warehouse_id
		df_drops['loading'] = False
		df_drops['unloading'] = False
		df_drops['price'] = 0
		if df_drops['lr_number'] is None:
			df_drops['AutoLR'] = True
		else:
			df_drops['AutoLR'] = False
		#df_drops['pickup_address_id'] = pickup_address_id

		# df_drops['customer_from_state'] = customer_from_state
		# df_drops['customer_from_addressname'] = customer_from_addressname
		# df_drops['customer_from_city'] = customer_from_city
		#df_drops['vendor_from_state'] = vendor_from_state
		df_drops['drop_address_id'] = 0
		df_drops['destination_code'] = ''
		if len(pd.unique(df_drops['branch'])) > 1 or len(pd.unique(df_drops['warehouse']))  > 1 or len(pd.unique(df_drops['customer'])) > 1 :
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Multiple warehouses/branch/customer defined." , "rejectdata":[],"ids":[] }'))
		#print(customer,vendor,warehouse,branch_id,customer_id,carrier_id,warehouse_id,pickup_address_id,pickup_address,customer_from_state,customer_from_addressname,customer_from_city)

		print('checking if its converted to df \n',df_drops)
		
		print("############################1##############################")
		#datasets = ['state*','city*','location*','customer_from_state','customer_from_city','customer_from_addressname','vendor','warehouse','branch','customer','vendor','material_type*','destination/consignee*']
		datasets = ['state*','city*','location*','warehouse','branch','customer','material_type*','destination/consignee*']
		#print(branch,customer,warehouse)

		for j in df_drops.index:
			if len(df_drops.loc[j,'state*']) and len(df_drops.loc[j,'city*']) and len(df_drops.loc[j,'location*']) and len(df_drops.loc[j,'material_type*']) and len(df_drops.loc[j,'destination/consignee*'])  <= 0:
				df_drops = df_drops.drop(index = j)
				print("dropped because mand fields not entered")
				
		# validate if duplicate lr_numbers is entered
		dup_lr = df_drops.duplicated(subset=['lr_number']).any()
		#print(dup_lr, end='\n\n') # True
		if dup_lr is True:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Duplicate lr_numbers entered in excel sheet." , "rejectdata":[],"ids":[] }'))
			df_disapproved = df_drops
			df_drops = df_drops[0:0]
		df_drops_copy = df_drops.copy(deep = True)
		for columns in datasets:
			#print(columns)
			df_drops_copy[columns] = df_drops_copy[columns].str.upper()
			df_drops[columns] = df_drops[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.replace(' ','')
		#raise("stop 1")

		df_approved  = df_approved[0:0]
		for i in df_drops.index:
			
			query1='''select id,REPLACE(UPPER(material),' ','') from material_type'''
			cur1.execute(query1)
			result1 = cur1.fetchall()
			material_id_df=pd.DataFrame(result1,columns = ['material_type_id','material_type*'])
			print("**************customer_id*************",df_drops.loc[0,'customer_id'])
			query2='''select REPLACE(UPPER(customer_lr_number),' ','') from customer_lr_numbers 
			where status = 'Used' and customer_id = {}'''.format(df_drops.loc[0,'customer_id'])
			print(query2)
			cur1.execute(query2)
			result2 = cur1.fetchall()
			if len(result2) <= 0:
				customer_lr_numbers = pd.DataFrame()
				customer_lr_numbers['customer_lr_number']=''
			else:
				customer_lr_numbers = pd.DataFrame(result2,columns = ['customer_lr_number'])

			
			query = """ select * from (select id,contact_code,upper(replace(address,' ','')) as address 
			from customeraddress where customer_id = {0})a 
			where address like '{1}' """.format(df_drops.loc[i,'customer_id'],df_drops_copy.loc[i,'destination/consignee*'])
			cur1.execute(query)
			print("++++++++++++++++++++",query)
			drop_address_id = cur1.fetchone()
			print('drop_address_id:',drop_address_id)
			try:
				print(drop_address_id[0])
				df_drops.loc[i,'drop_address_id'] = drop_address_id[0]
				df_drops.loc[i,'destination_code'] = drop_address_id[1]
			except:
				print("entering exception for no drop_address_id")
				answer = 'Entered location in the row number {0} is invalid.'.format(i+1)
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'Reason'] = 'Entered location is invalid.'
			
			query7=""" select * from (select id,REPLACE(UPPER(material),' ','') as material_type from material_type)a where material_type like '{0}%' """.format(df_drops_copy.loc[i,'material_type*'])
			cur1.execute(query7)
			result7 = cur1.fetchone()
			try:
				df_drops.loc[i,'material_type_id'] = result7[0]
				print("material type id==>",df_drops.loc[i,'material_type_id'])
			except:
				answer1 = 'Entered Material in the row number {0} is invalid.'.format(i+1)
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)	
				df_disapproved.loc[i,'Reason']='Entered Material is invalid.'
			
			
			query_check="""
			SELECT * FROM (select UPPER(REPLACE(state,' ',''))as state, UPPER(REPLACE(city,' ',''))as city,UPPER(REPLACE(location,' ',''))as location from city_state)a
			where state like '{}%' and city like '{}%' and location like '{}%' """.format(df_drops_copy.loc[i,'state*'],df_drops_copy.loc[i,'city*'],df_drops_copy.loc[i,'location*'])
			cur1.execute(query_check)
			query_check = cur1.fetchall()
			
			df_drops['lr_number'] = df_drops['lr_number'].astype(str)
			if len(query_check) > 0:
				print('entered address is right')
				print("printing lr_number column: \n",df_drops.loc[i,'lr_number'],type(df_drops.loc[i,'lr_number']))
				if df_drops.loc[i,'lr_number'] is None or df_drops.loc[i,'lr_number']=='nan':
					print("entering for lr being null")
					df_drops.loc[i,'lr_number'] = ""
				if ( df_drops.loc[i,'lr_number'] not in customer_lr_numbers['customer_lr_number']):
					df_drops['material_type*'] = df_drops_copy['material_type*'].str.replace(' ','')
					#print(df_drops.loc[i,'material_type'],material_id_df['material_type'])
					if material_id_df['material_type*'].str.contains(df_drops.loc[i,'material_type*']).any():
						if df_drops.loc[i,'drop_address_id'] is not None and df_drops.loc[i,'material_type_id'] is not None:
							row=df_drops.loc[[i]]
							df_approved = df_approved.append(row)
						
						else:
							print('Wrong entry of Destination/Wrong entry of material type.')
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							df_disapproved.loc[i,'Reason']='Wrong entry of Destination/Wrong entry of material type.'

					else:
						print('Material type')
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
						df_disapproved.loc[i,'Reason']='Wrong entry of material type.'
				else:
					print('incorrect lr_number')
					row=df_drops.loc[[i]]
					df_disapproved = df_disapproved.append(row)
					df_disapproved.loc[i,'Reason']='lr_number already in use.'
			else:
				print('to location incorrect')
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'Reason']='Location incorrect.'

		
		json_output = json.loads('{"drops":[], "success":"", "message":"" , "rejectdata":[] }')
		df_approved.rename(columns = {"destination/consignee*":"drop_address"}, inplace = True)
		df_approved.rename(columns = {"state*":"state"}, inplace = True)
		df_approved.rename(columns = {"city*":"city"}, inplace = True)
		df_approved.rename(columns = {"location*":"location"}, inplace = True)
		df_approved.rename(columns = {"actual_weight*":"actual_weight"}, inplace = True)
		df_approved.rename(columns = {"material_type*":"material_type"}, inplace = True)
		if len(df_approved) > 0 :

			df_approved['invoice_date'] = pd.to_datetime(df_approved['invoice_date']).dt.strftime('%d/%m/%Y')
			df_approved['invoice_date']=df_approved['invoice_date'].astype(str)
			df_approved[['drop_address_id','material_type_id']] = df_approved[['drop_address_id','material_type_id']].astype(int)

			print('after merge \n',df_approved)
			print("column names in rej : ",df_disapproved)
			df_disapproved = df_disapproved.drop_duplicates(subset = ['state*','city*','location*'] ,keep = 'last')
			print("len of df_approved and df_disapproved",len(df_approved),len(df_disapproved))
			df_approved = df_approved.reset_index(drop=True)
			df_approved['id'] = df_approved.index
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['drops']=json_load_final
			#json_output['ids']=json_load_id
			json_output['success']="true"
			json_output['message']='Success'

		else:
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_output['success']="false"
			json_output['message']="unsuccessful"
		print("len(df_disapproved):",len(df_disapproved))
		if len(df_disapproved)>0 and len(df_approved) > 0:
			print("entering for df_approved and df_disapproved len > 0")
			json_output['success']="true"
			json_output['message']="partial"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
	except:
		logging.info("An exception was thrown!", exc_info=True)
		return jsonify(json.loads('{"data":[], "success":"false", "message":"Invalid data." , "rejectdata":[],"ids":[] }'))
		print('ok')
	finally:
		cur1.close()
		conn1.close()
		
	return jsonify(json_output)


@app.route("/invoice_generation",methods = ['POST'])
def invoice_generation():
	try:
		def default(obj):
			if isinstance(obj, Decimal):
				return str(obj)
			#raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)
			
		content = request.get_json()
		trip_id=content["trip_id"]

		con=psycopg2.connect(
				host="127.0.0.1",
				database="ezyloads",
				user="ezyloads",
				password="ezy@1234"
		)
		cur=con.cursor()
		zenqore_query="""select 
		trip_track.trip_id as trip_id,
		shipment_details.customer_lr_number,
		company.company_name as organization_name,
		--company.logo as organization_logo,
		company.gst as organization_gstin_desc,
		company.street_address  as organization_address ,
		company.city as organization_city ,
		company.pincode as organization_pincode,
		company.gst as organization_gstin,
		drops.name as consignee_name,
		SUBSTRING(drops.address, 1, 35) as consignee_address ,
		drops.company_name as consignee_company,
		t.to_location as consignee_city,
		drops.pincode as consignee_pincode,
		t.gst_number as consignee_gstin,
		'' as consignee_gstin_desc,
		(case when booking_commercial.logistic_booking_type != 'LTL' then vehicle.regno else '' end) as vehicle_number,
		shipment_details.ewaybillno,
		shipment_details.invoice_no as doc_details_number,
		to_char(one.invoice_date,'DD-MON-YYYY') as doc_details_date,
		to_char(one.valid_upto,'DD-MON-YYYY') as doc_details_due_date,
		to_char(two.ewaybill_date,'DD-MON-YYYY')as ewaybill_date,
		company.company_name as ewaybill_generatedBy,
		company.carrier_company_code as ewaybill_transporter_id,
		to_char(two.valid_upto,'DD-MON-YYYY') as ewaybill_validTill ,
		'' ewaybill_valid_by,
		'' as description_name,
		'' as hsn_code,
		'' as particular,
		sum(trip_consignment_package_details.no_of_box) as quantity,
		booking_commercial.carrier_price_per_kg as unit_price,
		booking_commercial.carrier_cgst as cgst,
		booking_commercial.carrier_sgst as sgst,
		'' as discount,
		booking_commercial.carrier_total_freight as total_price,
		'' as item_total,
		'' as taxable_amount,
		'' as tax_amount,
		'' as total_amount,
		'' as total_in_words,
		'' as notes,
		E'Errros and ommissions \n
	Interest at @ 12 % pa for delalayed payment \n
	Disputes if any shall be subject to seller place of jurisdiction ' as tnc,
		'' as for_name,
		'' as signatue

	from 
	trip_track
	join shipment_details on shipment_details.drop_id = trip_track.drop_id
	join drops on drops.id = trip_track.drop_id
	join customeraddress on customeraddress.id = drops.customeraddress_id
	join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
	join trip_consignment_package_details on trip_consignment.trip_consignment_id = trip_consignment_package_details.trip_consignment_id
	join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
	join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
	join booking on booking.id = vehicle_booking_details.booking_id
	join warehouse on booking.warehouse_id::int = warehouse.id
	join branch on warehouse.branch_id = branch.id
	join customer on customeraddress.customer_id = customer.id
	join company on vehicle.company_id = company.id
	inner join source on booking.source_id = source.id
	join (select city as from_location,id from customeraddress)s on s.id = source.customeraddress_id
	join (select city as to_location,id,gst_number from customeraddress)t on t.id = drops.customeraddress_id
	left join (select trip_id,added_on_datetime::date as invoice_date,valid_upto from trip_documents where trip_documents.document_type = 'invoice' group by trip_id,added_on_datetime,valid_upto)one on one.trip_id= trip_track.trip_id
	left join (select trip_id,added_on_datetime::date as ewaybill_date,valid_upto from trip_documents where trip_documents.document_type = 'ewaybill' group by trip_id,added_on_datetime,valid_upto)two on one.trip_id= trip_track.trip_id
	where trip_track.trip_id = {}
	group by 

	shipment_details.customer_lr_number,
		company.company_name,
		--company.logo,
		company.gst,
		company.street_address,
		company.city,
		company.pincode,
		company.gst,
		drops.name,
		drops.address,
		drops.company_name,
		drops.pincode,
		vehicle.regno,
		shipment_details.ewaybillno,
		booking_commercial.logistic_booking_type,
		t.to_location,
		t.gst_number,
		booking_commercial.logistic_booking_type,
		shipment_details.invoice_no,
		one.invoice_date,
		one.valid_upto,
		two.ewaybill_date,
		company.company_name,
		company.carrier_company_code,
		two.valid_upto,
		trip_consignment_package_details.no_of_box,
		booking_commercial.carrier_price_per_kg,
		booking_commercial.carrier_cgst,
		booking_commercial.carrier_sgst,
		booking_commercial.carrier_total_freight,
		trip_track.trip_id
		
	order by trip_track.trip_id desc limit 1 """.format(trip_id)

		cur.execute(zenqore_query)
		query = cur.fetchall()
		zenqore_invoice_sales_payload= pd.DataFrame(query,columns=['trip_id','organization_name','organization_logo','organization_gstin_desc','organization_address','organization_city','organization_pincode','organization_gstin','consignee_name','consignee_address','consignee_company','consignee_city','consignee_pincode','consignee_gstin','consignee_gstin_desc','vehicle_number','ewaybillno','doc_details_number','doc_details_date','doc_details_due_date','ewaybill_date','ewaybill_generatedBy','ewaybill_transporter_id','ewaybill_validTill','ewaybill_valid_by','description_name','hsn_code','particular','quantity','unit_price','cgst','sgst','discount','total_price','item_total','taxable_amount','tax_amount','total_amount','total_in_words','notes','tnc','for_name','signatue'])

		zenqore_invoice_sales_payload.fillna("",inplace=True)
		print(zenqore_invoice_sales_payload)
		json_output = json.loads('{ "data":[],"success":"true", "message":"success" }')
		zenqore_invoice_sales = json.loads(zenqore_invoice_sales_payload.to_json(orient='records'))
		json_output['data']=zenqore_invoice_sales
		#return (json_output)
		organization_name = zenqore_invoice_sales_payload['organization_name'][0]
		organization_address_1=zenqore_invoice_sales_payload['organization_address'][0]
		organization_address_2=zenqore_invoice_sales_payload['organization_address'][0]
		organization_address_3=zenqore_invoice_sales_payload['organization_address'][0]
		organization_City=zenqore_invoice_sales_payload['organization_city'][0]
		organization_pincode=str(zenqore_invoice_sales_payload['organization_pincode'][0])
		organization_gst=zenqore_invoice_sales_payload['organization_gstin'][0]
		organization_gstin_desc=zenqore_invoice_sales_payload['organization_gstin_desc'][0]
		organization_logo=zenqore_invoice_sales_payload['organization_logo'][0]
		
		doc_details_num=zenqore_invoice_sales_payload['doc_details_number'][0]
		doc_detailsDate=zenqore_invoice_sales_payload['doc_details_date'][0]
		doc_detailsDue=zenqore_invoice_sales_payload['doc_details_due_date'][0]
		
		bill_to_name=zenqore_invoice_sales_payload['consignee_name'][0]
		bill_to_address1=zenqore_invoice_sales_payload['consignee_address'][0]
		bill_to_address2=zenqore_invoice_sales_payload['consignee_address'][0]
		bill_to_address3=zenqore_invoice_sales_payload['consignee_address'][0]
		bill_to_address_city=zenqore_invoice_sales_payload['consignee_city'][0]
		bill_to_address_pincode=str(zenqore_invoice_sales_payload['consignee_pincode'][0])
		bill_to_address_gstin=zenqore_invoice_sales_payload['consignee_gstin'][0]
		bill_to_address_gstin_desc=zenqore_invoice_sales_payload['consignee_gstin_desc'][0]
		
		ship_to_name=zenqore_invoice_sales_payload['consignee_name'][0]
		ship_to_address1=zenqore_invoice_sales_payload['consignee_address'][0]
		ship_to_address2=zenqore_invoice_sales_payload['consignee_address'][0]
		ship_to_address3=zenqore_invoice_sales_payload['consignee_address'][0]
		ship_to_address_city=zenqore_invoice_sales_payload['consignee_city'][0]
		ship_to_address_pincode=str(zenqore_invoice_sales_payload['consignee_pincode'][0])
		ship_to_address_gstin=zenqore_invoice_sales_payload['consignee_gstin'][0]
		ship_to_address_gstin_desc=zenqore_invoice_sales_payload['consignee_gstin_desc'][0]
		
		eway_bill_number=zenqore_invoice_sales_payload['ewaybillno'][0]
		vehicle_number=zenqore_invoice_sales_payload['vehicle_number'][0]
		print("Total amount: ",zenqore_invoice_sales_payload['total_price'][0])
		total_amount_in_words = num2words(int(zenqore_invoice_sales_payload['total_price'][0]))
		login_url="http://52.172.7.210:3000/auth/login"
		payload = {"contact":"guhanraja.murugavel@zenqore.com",  
			"password":"f0228b3fcfcc73c0ed6e7087f0d871560c4bbc9c36b19157216089922c5b29718f2a1f7b82b5b1b1f73c4ae842719352a4151b5358092ba448971bc9829a27e0c97017978c50c41c567fbc8e307c29f249b896c5cc405062048962d7f2c0be52e86764cdc4a3a71013",
			"origin":"http://localhost:3002"}
		RESPONSE= requests.request("POST",login_url,data=payload)
		authtoken=RESPONSE.json()["auth_token"]

		getUserProfile_url="http://52.172.7.210:3000/master/get/getUserProfile" 
		payload={"authToken":authtoken}
		RESPONSE= requests.request("POST",getUserProfile_url,data=payload)
		getdata=RESPONSE.json()["data"]

		header={"Content-Type":"application/json","Authorization":authtoken}
		login_url_invoicesales="http://52.172.7.210:3000/invoices/sale/6024c58690dcd96268c9625b"

		# payload1={
		# "doc_type": "Tax Invoice",
		# "organization": {
		# "name": organization_name, "address1": organization_address_1,"address2":"","address3":"","city":organization_City,"pincode":organization_pincode,"gstin":organization_gst,"gstin_desc":organization_gstin_desc,"logo":{"data":"","type":""}},
		# "doc_details":{"number":doc_details_num,"date":doc_detailsDate,"due_date":doc_detailsDue},"bill_to":{"name":bill_to_name,"address1":bill_to_address1,"address2":"","address3":"","city":bill_to_address_city,"pincode":bill_to_address_pincode,"gstin":bill_to_address_gstin,"gstin_desc":bill_to_address_gstin_desc},"ship_to":{"name":ship_to_name,"address1":ship_to_address1,"address2":"","address3":"","city":ship_to_address_city,"pincode":ship_to_address_pincode,"gstin":ship_to_address_gstin,"gstin_desc":ship_to_address_gstin_desc},"eway_bill":{"number":eway_bill_number,"date":zenqore_invoice_sales_payload['ewaybill_date'][0],"vehicle_number":vehicle_number,"generated_by":zenqore_invoice_sales_payload['ewaybill_generatedBy'][0],"transporter_id":" ","valid_from":zenqore_invoice_sales_payload['ewaybill_generatedBy'][0],"valid_till":zenqore_invoice_sales_payload['ewaybill_validTill'][0]},
		# "description":[{"number":"description number","particulars":"description particulars","hsn_sac":"","quantity":{"qnumber":zenqore_invoice_sales_payload['quantity'][0],"qunit":"kg"},"unit_price":zenqore_invoice_sales_payload['unit_price'][0],"discount": "","total_price":zenqore_invoice_sales_payload['total_price'][0],"sgst":zenqore_invoice_sales_payload['sgst'][0],"cgst":zenqore_invoice_sales_payload['cgst'][0],"item_total":"1"}],
		# "taxable_amount":"","tax_amount":"","total_amount":zenqore_invoice_sales_payload['total_price'][0],"total_in_words":total_amount_in_words,"payment_link":"","notes":["",""],"tnc":["Errros and ommissions","Interest at @ 12 % pa for delalayed payment","Disputes if any shall be subject to seller place of jurisdiction"],"for_name":ship_to_name,"signature":{"base64":{"data":"","type":""}},"signatory_name":""} 

		payload1={
		"doc_type": "Tax Invoice",
		"organization": {
		"name": "1", "address1": "2","address2":"3","address3":"4","city":"5","pincode":"6","gstin":"7","gstin_desc":"8","logo":{"data":"9","type":"10"}},
		"doc_details":{"number":"11","date":"12","due_date":"13"},"bill_to":{"name":"14","address1":"15","address2":"16","address3":"17","city":"18","pincode":"19","gstin":"20","gstin_desc":"21"},"ship_to":{"name":"22","address1":"23","address2":"24","address3":"25","city":"26","pincode":"27","gstin":"28","gstin_desc":"29"},"eway_bill":{"number":"30","date":"31","vehicle_number":"32","generated_by":"33","transporter_id":"34","valid_from":"35","valid_till":"36"},
		"description":[{"number":"37","particulars":"38","hsn_sac":"39","quantity":{"qnumber":"40","qunit":"41"},"unit_price":"42","discount": "43","total_price":"44","sgst":"45","cgst":"46","item_total":"47"}],
		"taxable_amount":"48","tax_amount":"49","total_amount":"50","total_in_words":"51","payment_link":"52","notes":["53","54"],"tnc":["Errros and ommissions","Interest at @ 12 % pa for delalayed payment","Disputes if any shall be subject to seller place of jurisdiction"],"for_name":"55","signature":{"base64":{"data":"56","type":"57"}},"signatory_name":"58"} 

		#json_object=json.dumps(Dict,indent=4)
		#print(json_object)
		#print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2',payload1)
		#print("#########################################",df)
		#print(json_output)
		#header={"Content-Type":"application/json","Authorization":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjoiZ3VoYW5yYWphLm11cnVnYXZlbEB6ZW5xb3JlLmNvbSIsIlVzZXJJZCI6IjYwNGI2ZjQxZjY4YmU3ZDYxZTQ3MWFiZCIsImlhdCI6MTYxNzM2MDAwMn0.qTWB_oIe1qS4i0PFEZh1SVTtVGkQjQaophBE2wxiAhk"}
		#login_url_invoicesales="http://52.172.7.210:3000/invoices/sale/6024c58690dcd96268c9625b"
		data_v = json.dumps(payload1,default = default)
		RESPONSE= requests.post(login_url_invoicesales,data=data_v,headers=header)
		solution = 'authtoken=RESPONSE.json()["auth_token"]'
		solution = json.dumps(json.loads(RESPONSE.text))
	#return jsonify(json_output)	
	except Exception as e:
		print(e)
	finally:
		cur.close()
		con.close()
		
	return jsonify(RESPONSE.json())


@app.route('/ltl_customer_commercial', methods = ['POST'])

def ltl_customer_commercial():
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		print('Hi 21feb2021')
		
		content = request.get_json()
		company_id = content["company_id"]
		f = request.files["uploadfile"]
		df_drops = pd.read_excel(f.stream,engine = 'openpyxl')
		df_drops.dropna(how = 'all',inplace = True)
		if not 'from_zone' in  df_drops.columns:
			#location
			validation_columns = ['branch','warehouse','customer','from_state','from_city','from_location','to_state','to_city','to_location','Conversion Factor','Slab/Fixed/Lumpsum','size','UOM From','UOM To','price_per_kg','fov','fsc','oda','Docket Charges','Handling charges','Management fee','distance_km','tat']
			
			check =  all(item in df_drops.columns for item in validation_columns)
			if check is False:
				return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
		else:
			validation_columns = ['branch','warehouse','customer','from_zone','to_zone','Conversion Factor','Slab/Fixed/Lumpsum','size','UOM From','UOM To','Rate/ Kg','FOV','FSC','ODA','Docket Charges','Handling charges','Management fee','Distance In Km','TAT in Days']
			
			check =  all(item in df_drops.columns for item in validation_columns)
			if check is False:
				return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
		
		if len(pd.unique(df_drops['branch'])) > 1 or len(pd.unique(df_drops['warehouse']))  > 1 or len(pd.unique(df_drops['customer'])) > 1 :
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Multiple warehouses/branch/customer defined." , "rejectdata":[],"ids":[] }'))
			
		print('checking if its converted to df \n',df_drops)
		

		df_approved = df_drops[0:0]
		df_disapproved = df_drops[0:0]
		datasets = ['customer','warehouse','branch','Slab/Fixed/Lumpsum']
		
		df_drops_copy = df_drops.copy()
		for columns in datasets:
			df_drops[columns] = df_drops[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.replace(" ","")
		#df_drops['Slab/Fixed/Lumpsum'] = df_drops['Slab/Fixed/Lumpsum'].str.higher()
		print(df_drops['Slab/Fixed/Lumpsum'])
		for i in df_drops.index:
			if df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'SLAB' or df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'FIXED' :
				df_drops.loc[i,'size'] = ''
				df_drops_copy.loc[i,'size'] = ''

		query1='''select city,state,location,id from city_state'''
		cur1.execute(query1)
		result = cur1.fetchall()
		result=pd.DataFrame(result,columns = ['city','state','location','id'])

		
		query3="""select * from( select id,upper(replace(customer_company,' ',''))as customer_company from customer)a where customer_company like '{0}' and company_id = {1} """.format(df_drops_copy.loc[0,'customer'],company_id)
		cur1.execute(query3)
		result3 = cur1.fetchall()
		if len(result3)<= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong Entry of Customer." , "rejectdata":[],"ids":[] }'))
		customer_id_df =pd.DataFrame(result3,columns = ['customer_id','customer'])
		

		query4="""select * from (select branch.id as branch_id,upper(replace(branch.branch_name,' ','')) as branch_name,warehouse.id as warehouse_id, upper(replace(warehouse_name,' ','')) as warehouse from branch join warehouse on branch.id = warehouse.branch_id)a where warehouse like '{0}' and branch_name like '{1}' """.format(df_drops_copy.loc[0,'warehouse'],df_drops_copy.loc[0,'branch'])

		cur1.execute(query4)
		result4 = cur1.fetchall()
		if len(result4)<= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong Entry of Customer." , "rejectdata":[],"ids":[] }'))
		warehouse_id_df =pd.DataFrame(result4,columns = ['branch_id','branch_name','warehouse_id','warehouse'])

		query5="""SELECT concat(replace(upper(customer.customer_company),' ',''),
			',',
			replace(upper(ltl_cust_master.to_state),' ',''),
			',',
			replace(upper(ltl_cust_master.to_city),' ',''),
			',',
			replace(upper(ltl_cust_master.to_location),' ',''),
			',',
			replace(upper(ltl_cust_master.from_state),' ',''),
			',',
			replace(upper(ltl_cust_master.from_city),' ',''),
			',',
			replace(upper(ltl_cust_master.from_location),' ','')) 
			FROM "ltl_cust_master" join customer on ltl_cust_master.customer_id = customer.id where warehouse_id = {0} and customer.company_id = {1}; """.format(warehouse_id_df.loc[0,'warehouse_id'],company_id)
		cur1.execute(query5)
		result5 = cur1.fetchall()
		if len(result5)<= 0:
			dup =pd.DataFrame()
			dup['dup']=''
		else:
			dup =pd.DataFrame(result5,columns = ['dup'])
		#print('dup \n',list(dup['dup']))

		query6="""SELECT concat(
replace(upper(customer.customer_company),' ',''),
',',
replace(upper(ltl_cust_master.from_zone),' ',''),
',',
replace(upper(ltl_cust_master.to_zone),' ','')) 
FROM "ltl_cust_master" join customer on ltl_cust_master.customer_id = customer.id where rate_type = 'zone' and warehouse_id = {0} and customer.company_id = {1};""".format(warehouse_id_df.loc[0,'warehouse_id'],company_id)
		cur1.execute(query6)
		result6 = cur1.fetchall()
		if len(result6) <= 0:
			dup_zone =pd.DataFrame()
			dup_zone['dup'] = ''
		else:
			dup_zone =pd.DataFrame(result6,columns = ['dup'])
		
		#print(warehouse_id_df)
		
		#print("checking length \n ",len(result))

		#print('checking result \n',result)
		datasets2 = ['state','city','location']
		for columns in datasets2:
			result[columns] = result[columns].str.upper()
		
		if not 'from_zone' in  df_drops.columns:
			df_drops['rate_type'] = 'location'
			
			df_drops['from_zone'] = ''
			df_drops['to_zone'] = ''
			print('entering for location rate_type')
			datasets = ['to_state','to_city','to_location','from_state','from_city','from_location','customer']
			for columns in datasets:
				df_drops[columns] = df_drops[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.replace(" ","")
			df_drops['Slab/Fixed/Lumpsum'] = df_drops['Slab/Fixed/Lumpsum'].str.upper()
			dup_list = list(dup['dup'])
			#print("dup_list ",dup_list)
			df_drops_copy['dup_check'] = df_drops_copy[['customer','to_state','to_city','to_location','from_state','from_city','from_location']].agg(','.join, axis=1)
			
			for i in df_drops.index:
				if df_drops_copy.loc[i,'dup_check'] not in dup_list:
					print('entering for no commercials')
					
					query_check="""
					SELECT * 
FROM (
	select replace(upper(state),' ','')as state,
	replace(upper(city),' ','') as city,
	replace(upper(location),' ','') as location 
	from city_state)a where state like '{0}' and city like '{1}' and location like '{2}' """.format(df_drops_copy.loc[i,'to_state'],df_drops_copy.loc[i,'to_city'],df_drops_copy.loc[i,'to_location'])
					cur1.execute(query_check)
					
					query_check = cur1.fetchall()
					
					query_check2="""SELECT * 
FROM (
	select replace(upper(state),' ','')as state,
	replace(upper(city),' ','') as city,
	replace(upper(location),' ','') as location 
	from city_state)a  where state like '{0}' and city like '{1}' and location like '{2}'
					""".format(df_drops_copy.loc[i,'from_state'],df_drops_copy.loc[i,'from_city'],df_drops_copy.loc[i,'from_location'])
					cur1.execute(query_check2)
					print("checking the query \n",query_check2)
					query_check2 = cur1.fetchall()
					print('length of location query:',len(query_check),len(query_check2))

					if len(query_check) and len(query_check2) > 0:
						print('location correct')
						
						
					#if (df_drops.loc[i,'validation_to_location'] and df_drops.loc[i,'validation_from_location'] in l):
						print("entering for location")
						slab_fixed_lumpsum = ['slab','fixed','lumpsum']
						if df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'SLAB'or df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'FIXED':
							print('it is slab or fixed')
							df_drops.loc[i,'size'] = ''
							if df_drops.loc[i,'UOM From'] and df_drops.loc[i,'UOM To'] is not np.nan:
								print('entering for uom')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('uom empty')
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
						elif df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'LUMPSUM':
							print('entering for lumpsum')
							size = ['SMALL','MEDIUM','LARGE']
							try:
								df_drops['size'] = df_drops['size'].str.upper()
							except:
								return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong size entry." , "rejectdata":[],"ids":[] }'))
							if df_drops.loc[i,'size'] in size:
								print('size input is correct')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('size input is incorrect')
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
								flag = 1
						else:
							print('not slab or fixed or lumpsum')
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							flag = 1
						#row=df_drops.loc[[i]]
						#df_approved = df_approved.append(row)
					else:
						print('location not correct')
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
				else:
					print('commercials already exists.')
					row=df_drops.loc[[i]]
					df_disapproved = df_disapproved.append(row)
			print("length before removing duplicate:",len(df_approved))
			df_approved = df_approved.drop_duplicates(subset = ['from_state','to_state','from_city','to_city','from_location','to_location','customer','warehouse'] ,keep = 'last')
			print("length after removing duplicate:",len(df_approved))
		else :
			df_drops['rate_type'] = 'zone'
			df_drops_copy = df_drops.copy()
			
			df_drops['from_state'] = ''
			df_drops['from_city'] = ''
			df_drops['from_location'] = ''
			df_drops['to_state'] = ''
			df_drops['to_city'] = ''
			df_drops['to_location'] = ''
			print("*****************************************************")
				
			query2='''select * from customer_zone where customer_id = {0} and warehouse_id = {1}'''.format(customer_id_df.loc[0,'customer_id'],warehouse_id_df.loc[0,'warehouse_id'])
			cur1.execute(query2)
			zone_result = cur1.fetchall()
			if len(zone_result)<= 0:
				zone_result=pd.DataFrame()
				zone_result['id'],zone_result['zone'],zone_result['state']='','',''
			else:
				zone_result=pd.DataFrame(result,columns = ['id','zone','state'])
			print("checking length \n ",len(result))

			datasets = ['to_zone','from_zone','Slab/Fixed/Lumpsum']
			for columns in datasets:
				df_drops[columns] = df_drops[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.replace(" ","")
			dup_list1 = list(dup_zone['dup'])
			
			#df_drops['to_zone'] = df_drops[['to_zone']].agg(','.join, axis=1)
			#df_drops['from_zone'] = df_drops[['from_zone']].agg(','.join, axis=1)
			df_drops_copy['dup_check'] = df_drops_copy[['customer','from_zone','to_zone']].agg(','.join, axis=1)
			#df_drops['to_location'] = df_drops[['to_state','to_city','to_location']].agg(','.join, axis=1)
			
			#l_zone = list(result[['zone']])
			for i in df_drops.index:
				if df_drops_copy.loc[i,'dup_check'] not in dup_list1:
					print('entering for no commercials-1')

					#print("ccccccccccccccccccccccccccc",customer_id_df.loc[0,'customer_id'])
					query_check="""
					SELECT * FROM (select customer_id,upper(replace(zone,' ',''))as zone,warehouse_id from customer_zone)a where zone like '{}' and customer_id = {} and warehouse_id = {}""".format(df_drops_copy.loc[i,'from_zone'],customer_id_df.loc[0,'customer_id'],warehouse_id_df.loc[0,'warehouse_id'])
					#print(query_check)
					cur1.execute(query_check)
					query_check = cur1.fetchall()
					
					query_check2="""
					SELECT * FROM (select customer_id,upper(replace(zone,' ',''))as zone,warehouse_id from customer_zone)a where zone like '{}' and customer_id = {} and warehouse_id = {}""".format(df_drops_copy.loc[i,'to_zone'],customer_id_df.loc[0,'customer_id'],warehouse_id_df.loc[0,'warehouse_id'])
					cur1.execute(query_check2)
					query_check2 = cur1.fetchall()
					
					if len(query_check) and len(query_check2) > 0:
						print('entering for correct zone')
						#print("len(df_drops_copy)",len(df_drops_copy))
						#for i in df_drops_copy.index:
						from_zone_states='''select state from customer_zone where customer_id = {} and zone like '{}' and warehouse_id = {}'''.format(customer_id_df.loc[0,'customer_id'],df_drops_copy.loc[i,'from_zone'],warehouse_id_df.loc[0,'warehouse_id'])
						cur1.execute(from_zone_states)
						from_zone_states = cur1.fetchall()
						if len(from_zone_states) <= 0:
							return jsonify(json.loads('{"data":[], "success":"false", "message":"Entered zone does not exit." , "rejectdata":[],"ids":[] }'))
						else:
							from_zone_states =pd.DataFrame(from_zone_states,columns = ['state'])
						from_states = list(from_zone_states['state'])
						#print(from_states)
						#raise("stop")
						#for i in df_drops_copy.index:
						to_zone_states='''select state from customer_zone where zone like '{}' and customer_id = {} and warehouse_id = {}'''.format(df_drops_copy.loc[i,'to_zone'],customer_id_df.loc[0,'customer_id'],warehouse_id_df.loc[0,'warehouse_id'])
						cur1.execute(to_zone_states)
						to_zone_states = cur1.fetchall()
						if len(to_zone_states) <= 0:
							return jsonify(json.loads('{"data":[], "success":"false", "message":"Entered zone does not exit." , "rejectdata":[],"ids":[] }'))
						else:
							to_zone_states =pd.DataFrame(to_zone_states,columns = ['state'])
						to_states = list(to_zone_states['state'])
						print("df_drops_copy.loc[0,'from_zone']->>df_drops_copy.loc[0,'to_zone']--->from_states-->to_states",df_drops_copy.loc[i,'from_zone'],df_drops_copy.loc[i,'to_zone'],from_states,to_states)
						for j in from_states:
							for k in to_states:
								
								query7 =""" select * from ltl_cust_master where from_state like '{}' and to_state like '{}' and status = 'Active' and customer_id = {} and warehouse_id = {} """.format(j,k,customer_id_df.loc[0,'customer_id'],warehouse_id_df.loc[0,'warehouse_id'])
								cur1.execute(query7)
								query7 = cur1.fetchall()
								#print("commercial duplication validation query7 \n",query7)
								if len(query7) >= 1:
									#return jsonify(json.loads('{"data":[], "success":"false", "message":"Commerical already exists in Location type." , "rejectdata":[],"ids":[] }'))
									print('There is a location commercials aleady assigned.')
									row=df_drops.loc[[i]]
									df_disapproved = df_disapproved.append(row)
									print("df_disapproved after dropping dup comm from loc vali in zone. \n",df_disapproved)
									#raise(stop)
						print("+++++++++++++++++++++++++++++++++++++++++++++++++++")
						from_states = []
						to_states = []
						
						
						
						
					#if (df_drops.loc[i,'to_zone'] and df_drops.loc[i,'from_zone'] in l_zone):
						slab_fixed_sumsum = ['slab','fixed','lumpsum']
						if df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'SLAB'or df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'FIXED':
							print('entering for slab or fixed')
							if df_drops.loc[i,'UOM From'] and df_drops.loc[i,'UOM To']  is not np.nan:
								print('entering for correct uom')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('entering for wrong uom')
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
						elif df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'LUMPSUM':
							print('entering for lumpsum')
							size = ['SMALL','MEDIUM','LARGE']
							try:
								df_drops['size'] = df_drops['size'].str.upper()
							except:
								return jsonify(json.loads('{"data":[], "success":"false", "message":"Size input format is incorrect." , "rejectdata":[],"ids":[] }'))
							if df_drops.loc[i,'size'] in size:
								print('entering for correct size')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('entering for wrong size:',df_drops.loc[i,'size'])
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
								flag = 1
						else:
							print('wrong slab/fixed/lumpsum',df_drops.loc[i,'Slab/Fixed/Lumpsum'])
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							flag = 1
					else:
						print('entering for wrong zone')
						#print("df_drops_copy.loc[0,'from_zone']->>df_drops_copy.loc[0,'to_zone']--->from_states-->to_states",df_drops_copy.loc[i,'from_zone'],df_drops_copy.loc[i,'to_zone'],from_states,to_states)
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
						flag = 1
			print("length of df_approved before removing duplicates",len(df_approved))
			df_approved = df_approved.drop_duplicates(subset = ['to_zone','from_zone','customer','warehouse'] ,keep = 'last')
			print("length of df_approved after removing duplicates",len(df_approved))
		
		json_output = json.loads('{"data":[], "success":"false", "message":"" , "rejectdata":[],"ids":[] }')
		if len(df_approved) > 0:
			
			df_approved['customer_id'] = customer_id_df.loc[0,'customer_id']
			print("if length is more than 1 \n",customer_id_df)
			df_approved['warehouse_id'] = warehouse_id_df.loc[0,'warehouse_id']
			df_approved['branch_id'] = warehouse_id_df.loc[0,'branch_id']
			#df_approved = df_approved.merge(result2,on='vehicle_type')
			df_approved_id = df_approved[['rate_type','customer_id','branch_id','warehouse_id']]
			print('after merge \n',df_approved_id)
			#df_approved.drop(['validation_from_location','validation_to_location'],axis = 1,inplace = True)
			print('------------------ \n',df_approved[['customer_id','branch_id','warehouse_id']])
			df_approved = df_approved.drop(['rate_type','customer_id','branch_id','warehouse_id'],axis = 1)
			
			df_approved_id = df_approved_id[:1]
			#print(df_approved_id)
			#print('after location type entry \n',df_approved)
			#print('disapproved ,after location type entry \n',df_disapproved)
			#df_approved = df_approved.dropna(axis = 0)
			df_approved.drop(['customer','warehouse','branch'],axis = 1,inplace = True)
			df_approved.rename(columns = {'Slab/Fixed/Lumpsum':'pricing_type'}, inplace = True)
			df_approved.rename(columns = {'Distance In Km':'distance_km'}, inplace = True)
			df_approved.rename(columns = {'Conversion Factor':'conv_factor'}, inplace = True)
			df_approved.rename(columns = {'TAT in Days':'tat'}, inplace = True)
			df_approved.rename(columns = {'UOM From':'from_cap'}, inplace = True)
			df_approved.rename(columns = {'UOM To':'to_cap'}, inplace = True)
			df_approved.rename(columns = {'Docket Charges':'docket_chrgs'}, inplace = True)
			df_approved.rename(columns = {'Handling charges':'handling_chrgs'}, inplace = True)
			df_approved.rename(columns = {'Management fee':'mgmt_fee'}, inplace = True)
			df_approved.rename(columns = {'FOV':'fov'}, inplace = True)
			df_approved.rename(columns = {'FSC':'fsc'}, inplace = True)
			df_approved.rename(columns = {'ODA':'oda'}, inplace = True)
			df_approved.rename(columns = {'Rate/ Kg':'price_per_kg'}, inplace = True)
			json_load_final_id=json.loads(df_approved_id.to_json(orient='records'))
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['data']=json_load_final
			json_output['ids']=json_load_final_id
			json_output['success']="true"
			json_output['message']="success"
		else:
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_output['success']="false"
			json_output['message']="unsuccessful"
		
		print("len(df_disapproved)",len(df_disapproved),len(df_approved))

		if len(df_disapproved)>0 and len(df_approved) > 0:
			df_disapproved = df_disapproved.drop_duplicates(subset = ['to_zone','from_zone','customer','warehouse'] ,keep = 'last')
			print("len(df_disapproved)",len(df_disapproved),len(df_approved))
			print("entering for partial")
			json_output['success']="true"
			json_output['message']="partial"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
		print(json_output)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"data":[], "success":"false", "message":"Unsuccessful" , "rejectdata":[], "ids":[] }')
	finally:
		cur1.close()
		conn1.close()
	return jsonify(json_output)

@app.route('/ltl_vendor_commercial', methods = ['POST'])
def ltl_vendor_commercial():
	try:
		print('Hi')
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		#content = request.get_json()
		f = request.files["uploadfile"]
		df_drops = pd.read_excel(f.stream,engine = 'openpyxl')
		df_drops.dropna(how = 'all',inplace = True)
		if not 'from_zone' in  df_drops.columns:
			#location
			validation_columns = ['branch','warehouse','vendor','from_state','from_city','from_location','to_state','to_city','to_location','Conversion Factor','Slab/Fixed/Lumpsum','size','UOM From','UOM To','price_per_kg','fov','fsc','oda','Docket Charges','Handling charges','Management fee','distance_km','tat']
			
			check =  all(item in df_drops.columns for item in validation_columns)
			if check is False:
				return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
		else:
			validation_columns = ['branch','warehouse','vendor','from_zone','to_zone','Conversion Factor','Slab/Fixed/Lumpsum','size','UOM From','UOM To','Rate/ Kg','FOV','FSC','ODA','Docket Charges','Handling charges','Management fee','Distance In Km','TAT in Days']
			
			check =  all(item in df_drops.columns for item in validation_columns)
			if check is False:
				return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
		
		if len(pd.unique(df_drops['branch'])) > 1 or len(pd.unique(df_drops['warehouse']))  > 1 or len(pd.unique(df_drops['vendor'])) > 1 :
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Multiple warehouses/branch/vendor defined." , "rejectdata":[],"ids":[] }'))
			
		print('checking if its converted to df \n',df_drops)
		

		df_approved = df_drops[0:0]
		df_disapproved = df_drops[0:0]
		datasets = ['vendor','warehouse','branch','Slab/Fixed/Lumpsum']
		df_drops.dropna(how = 'all',inplace = True)
		df_drops_copy = df_drops.copy()
		for columns in datasets:
			df_drops[columns] = df_drops[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.replace(" ","")
		#df_drops['Slab/Fixed/Lumpsum'] = df_drops['Slab/Fixed/Lumpsum'].str.higher()
		print(df_drops['Slab/Fixed/Lumpsum'])
		for i in df_drops.index:
			if df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'SLAB' or df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'FIXED' :
				df_drops.loc[i,'size'] = ''
				df_drops_copy.loc[i,'size'] = ''

		query1='''select city,state,location,id from city_state'''
		cur1.execute(query1)
		result = cur1.fetchall()
		result=pd.DataFrame(result,columns = ['city','state','location','id'])

		
		query3="""select * from( select id,upper(replace(company_name,' ',''))as carrier_company from carrier_company)a where carrier_company like '{0}' """.format(df_drops_copy.loc[0,'vendor'])
		cur1.execute(query3)
		result3 = cur1.fetchall()
		if len(result3)<= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong Entry of Vendor." , "rejectdata":[],"ids":[] }'))
		carrier_id_df =pd.DataFrame(result3,columns = ['carrier_id','vendor'])
		

		query4="""select * from (select branch.id as branch_id,upper(replace(branch.branch_name,' ','')) as branch_name,warehouse.id as warehouse_id, upper(replace(warehouse_name,' ','')) as warehouse from branch join warehouse on branch.id = warehouse.branch_id)a where warehouse like '{0}' and branch_name like '{1}' """.format(df_drops_copy.loc[0,'warehouse'],df_drops_copy.loc[0,'branch'])

		cur1.execute(query4)
		result4 = cur1.fetchall()
		if len(result4)<= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong Entry of Branch/Warehouse." , "rejectdata":[],"ids":[] }'))
		warehouse_id_df =pd.DataFrame(result4,columns = ['branch_id','branch_name','warehouse_id','warehouse'])

		query5="""SELECT concat(replace(upper(carrier_company.company_name),' ',''),
			',',
			replace(upper(ltl_carr_master.to_state),' ',''),
			',',
			replace(upper(ltl_carr_master.to_city),' ',''),
			',',
			replace(upper(ltl_carr_master.to_location),' ',''),
			',',
			replace(upper(ltl_carr_master.from_state),' ',''),
			',',
			replace(upper(ltl_carr_master.from_city),' ',''),
			',',
			replace(upper(ltl_carr_master.from_location),' ','')) 
			FROM "ltl_carr_master" join carrier_company on ltl_carr_master.carrier_id = carrier_company.id where warehouse_id = {0}; """.format(warehouse_id_df.loc[0,'warehouse_id'])
		cur1.execute(query5)
		result5 = cur1.fetchall()
		if len(result5)<= 0:
			dup =pd.DataFrame()
			dup['dup']=''
		else:
			dup =pd.DataFrame(result5,columns = ['dup'])
		#print('dup \n',list(dup['dup']))

		query6="""SELECT concat(
replace(upper(carrier_company.company_name),' ',''),
',',
replace(upper(ltl_carr_master.from_zone),' ',''),
',',
replace(upper(ltl_carr_master.to_zone),' ','')) 
FROM "ltl_carr_master" join carrier_company on ltl_carr_master.carrier_id = carrier_company.id where rate_type = 'zone' and warehouse_id = {0};""".format(warehouse_id_df.loc[0,'warehouse_id'])
		cur1.execute(query6)
		result6 = cur1.fetchall()
		if len(result6) <= 0:
			dup_zone =pd.DataFrame()
			dup_zone['dup'] = ''
		else:
			dup_zone =pd.DataFrame(result6,columns = ['dup'])
		
		#print(warehouse_id_df)
		
		#print("checking length \n ",len(result))

		#print('checking result \n',result)
		datasets2 = ['state','city','location']
		for columns in datasets2:
			result[columns] = result[columns].str.upper()
		
		if not 'from_zone' in  df_drops.columns:
			df_drops['rate_type'] = 'location'
			
			df_drops['from_zone'] = ''
			df_drops['to_zone'] = ''
			print('entering for location rate_type')
			datasets = ['to_state','to_city','to_location','from_state','from_city','from_location','vendor']
			for columns in datasets:
				df_drops[columns] = df_drops[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.replace(" ","")
			df_drops['Slab/Fixed/Lumpsum'] = df_drops['Slab/Fixed/Lumpsum'].str.upper()
			dup_list = list(dup['dup'])
			#print("dup_list ",dup_list)
			df_drops_copy['dup_check'] = df_drops_copy[['vendor','to_state','to_city','to_location','from_state','from_city','from_location']].agg(','.join, axis=1)
			
			for i in df_drops.index:
				if df_drops_copy.loc[i,'dup_check'] not in dup_list:
					print('entering for no commercials')
					
					query_check="""
					SELECT * 
	FROM (
	select replace(upper(state),' ','')as state,
	replace(upper(city),' ','') as city,
	replace(upper(location),' ','') as location 
	from city_state)a where state like '{0}' and city like '{1}' and location like '{2}' """.format(df_drops_copy.loc[i,'to_state'],df_drops_copy.loc[i,'to_city'],df_drops_copy.loc[i,'to_location'])
					cur1.execute(query_check)
					
					query_check = cur1.fetchall()
					
					query_check2="""SELECT * 
	FROM (
	select replace(upper(state),' ','')as state,
	replace(upper(city),' ','') as city,
	replace(upper(location),' ','') as location 
	from city_state)a  where state like '{0}' and city like '{1}' and location like '{2}'
					""".format(df_drops_copy.loc[i,'from_state'],df_drops_copy.loc[i,'from_city'],df_drops_copy.loc[i,'from_location'])
					cur1.execute(query_check2)
					print("checking the query \n",query_check2)
					query_check2 = cur1.fetchall()
					print('length of location query:',len(query_check),len(query_check2))

					if len(query_check) and len(query_check2) > 0:
						print('location correct')
					#if (df_drops.loc[i,'validation_to_location'] and df_drops.loc[i,'validation_from_location'] in l):
						print("entering for location")
						slab_fixed_lumpsum = ['slab','fixed','lumpsum']
						if df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'SLAB'or df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'FIXED':
							print('it is slab or fixed')
							df_drops.loc[i,'size'] = ''
							if df_drops.loc[i,'UOM From'] and df_drops.loc[i,'UOM To'] is not np.nan:
								print('entering for uom')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('uom empty')
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
						elif df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'LUMPSUM':
							print('entering for lumpsum')
							size = ['SMALL','MEDIUM','LARGE']
							try:
								df_drops['size'] = df_drops['size'].str.upper()
							except:
								return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong size entry." , "rejectdata":[],"ids":[] }'))
							if df_drops.loc[i,'size'] in size:
								print('size input is correct')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('size input is incorrect')
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
								flag = 1
						else:
							print('not slab or fixed or lumpsum')
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							flag = 1
						#row=df_drops.loc[[i]]
						#df_approved = df_approved.append(row)
					else:
						print('location not correct')
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
				else:
					print('commercials already exists.')
					row=df_drops.loc[[i]]
					df_disapproved = df_disapproved.append(row)
			df_approved = df_approved.drop_duplicates(subset = ['from_state','to_state','from_city','to_city','from_location','to_location','vendor','warehouse'] ,keep = 'last')

		else :
			df_drops['rate_type'] = 'zone'
			df_drops_copy = df_drops.copy()
			
			df_drops['from_state'] = ''
			df_drops['from_city'] = ''
			df_drops['from_location'] = ''
			df_drops['to_state'] = ''
			df_drops['to_city'] = ''
			df_drops['to_location'] = ''
			print("*****************************************************")
				
				
			query2='''select * from carrier_zone where carrier_id = {0}'''.format(carrier_id_df.loc[0,'carrier_id'])
			cur1.execute(query2)
			zone_result = cur1.fetchall()
			if len(zone_result)<= 0:
				zone_result=pd.DataFrame()
				zone_result['id'],zone_result['zone'],zone_result['state']='','',''
			else:
				zone_result=pd.DataFrame(result,columns = ['id','zone','state'])
			print("checking length \n ",len(result))
			
			datasets = ['to_zone','from_zone','Slab/Fixed/Lumpsum']
			for columns in datasets:
				df_drops[columns] = df_drops[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.upper()
				df_drops_copy[columns] = df_drops_copy[columns].str.replace(" ","")
			dup_list1 = list(dup_zone['dup'])
			
			#df_drops['to_zone'] = df_drops[['to_zone']].agg(','.join, axis=1)
			#df_drops['from_zone'] = df_drops[['from_zone']].agg(','.join, axis=1)
			df_drops_copy['dup_check'] = df_drops_copy[['vendor','from_zone','to_zone']].agg(','.join, axis=1)
			#df_drops['to_location'] = df_drops[['to_state','to_city','to_location']].agg(','.join, axis=1)
			
			#l_zone = list(result[['zone']])
			print("len(df_drops)",len(df_drops))
			for i in df_drops.index:
				if df_drops_copy.loc[i,'dup_check'] not in dup_list1:
					print('entering for no commercials-1')
					
					print("ccccccccccccccccccccccccccc",carrier_id_df.loc[0,'carrier_id'])
					query_check="""
					SELECT * FROM (select carrier_id,upper(replace(zone,' ',''))as zone,warehouse_id from carrier_zone)a where zone like '{}' and carrier_id = {} and warehouse_id={}""".format(df_drops_copy.loc[i,'from_zone'],carrier_id_df.loc[0,'carrier_id'],warehouse_id_df.loc[0,'warehouse_id'])
					print(query_check)
					cur1.execute(query_check)
					query_check = cur1.fetchall()
					
					query_check2="""
					SELECT * FROM (select carrier_id,upper(replace(zone,' ',''))as zone,warehouse_id from carrier_zone)a where zone like '{}' and carrier_id = {} and warehouse_id={}""".format(df_drops_copy.loc[i,'to_zone'],carrier_id_df.loc[0,'carrier_id'],warehouse_id_df.loc[0,'warehouse_id'])
					cur1.execute(query_check2)
					query_check2 = cur1.fetchall()
					

					if len(query_check) and len(query_check2) > 0:
						print('entering for correct zone')
					#if (df_drops.loc[i,'to_zone'] and df_drops.loc[i,'from_zone'] in l_zone):
						for i in df_drops_copy.index:
							try:
								print("from_states,to_states",from_states,to_states)
							except:
								pass
							from_zone_states='''select state from carrier_zone where carrier_id = {} and zone like '{}' and warehouse_id = {}'''.format(carrier_id_df.loc[0,'carrier_id'],df_drops_copy.loc[0,'from_zone'],warehouse_id_df.loc[0,'warehouse_id'])
							cur1.execute(from_zone_states)
							from_zone_states = cur1.fetchall()
							if len(from_zone_states) <= 0:
								return jsonify(json.loads('{"data":[], "success":"false", "message":"Entered zone does not exit." , "rejectdata":[],"ids":[] }'))
							else:
								from_zone_states =pd.DataFrame(from_zone_states,columns = ['state'])
							from_states = list(from_zone_states['state'])
							#print(from_states)
							#raise("stop")
							#for i in df_drops_copy.index:
							to_zone_states='''select state from carrier_zone where zone like '{}' and carrier_id = {} and warehouse_id = {} '''.format(df_drops_copy.loc[0,'to_zone'],carrier_id_df.loc[0,'carrier_id'],warehouse_id_df.loc[0,'warehouse_id'])
							cur1.execute(to_zone_states)
							to_zone_states = cur1.fetchall()
							if len(to_zone_states) <= 0:
								return jsonify(json.loads('{"data":[], "success":"false", "message":"Entered zone does not exit." , "rejectdata":[],"ids":[] }'))
							else:
								to_zone_states =pd.DataFrame(to_zone_states,columns = ['state'])
							to_states = list(to_zone_states['state'])
							print(from_states,to_states)
							for j in from_states:
								for k in to_states:
									
									query7 =""" select * from ltl_carr_master where from_state like '{}' and to_state like '{}' and status = 'Active' and carrier_id = {} and warehouse_id = {} """.format(j,k,carrier_id_df.loc[0,'carrier_id'],warehouse_id_df.loc[0,'warehouse_id'])
									cur1.execute(query7)
									query7 = cur1.fetchall()
									print("commercial duplication validation query7 \n",query7)
									if len(query7) >= 1:
										return jsonify(json.loads('{"data":[], "success":"false", "message":"Commerical already exists in Location type." , "rejectdata":[],"ids":[] }'))
										print('There is a location commercials aleady assigned.')
										row=df_drops.loc[[i]]
										df_disapproved = df_disapproved.append(row)
							print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
							from_states = []
							to_states = []
						
						
						slab_fixed_sumsum = ['slab','fixed','lumpsum']
						if df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'SLAB'or df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'FIXED':
							print('entering for slab or fixed')
							if df_drops.loc[i,'UOM From'] and df_drops.loc[i,'UOM To']  is not np.nan:
								print('entering for correct uom')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('entering for wrong uom')
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
						elif df_drops.loc[i,'Slab/Fixed/Lumpsum'] == 'LUMPSUM':
							print('entering for lumpsum')
							size = ['SMALL','MEDIUM','LARGE']
							try:
								df_drops['size'] = df_drops['size'].str.upper()
							except:
								return jsonify(json.loads('{"data":[], "success":"false", "message":"Size input format is incorrect." , "rejectdata":[],"ids":[] }'))
							if df_drops.loc[i,'size'] in size:
								print('entering for correct size')
								row=df_drops.loc[[i]]
								df_approved = df_approved.append(row)
							else:
								print('entering for wrong size:',df_drops.loc[i,'size'])
								row=df_drops.loc[[i]]
								df_disapproved = df_disapproved.append(row)
								flag = 1
						else:
							print('wrong slab/fixed/lumpsum',df_drops.loc[i,'Slab/Fixed/Lumpsum'])
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							flag = 1
					else:
						print('entering for wrong location')
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
						flag = 1
			df_approved = df_approved.drop_duplicates(subset = ['to_zone','from_zone','vendor','warehouse'] ,keep = 'last')
			
		
		json_output = json.loads('{"data":[], "success":"false", "message":"" , "rejectdata":[],"ids":[] }')
		
		if len(df_approved) > 0:
			df_approved['carrier_id'] = carrier_id_df.loc[0,'carrier_id']
			df_approved['branch_id']=warehouse_id_df.loc[0,'branch_id']
			df_approved['warehouse_id']=warehouse_id_df.loc[0,'warehouse_id']
			 
			#df_approved = df_approved.merge(result2,on='vehicle_type')
			df_approved_id = df_approved[['rate_type','carrier_id','branch_id','warehouse_id']]
			print('after merge \n',df_approved_id)
			#df_approved.drop(['validation_from_location','validation_to_location'],axis = 1,inplace = True)
			print('------------------ \n',df_approved[['carrier_id','branch_id','warehouse_id']])
			df_approved = df_approved.drop(['rate_type','carrier_id','branch_id','warehouse_id'],axis = 1)
			
			df_approved_id = df_approved_id[:1]
			#print(df_approved_id)
			#print('after location type entry \n',df_approved)
			#print('disapproved ,after location type entry \n',df_disapproved)
			#df_approved = df_approved.dropna(axis = 0)
			df_approved.drop(['vendor','warehouse','branch'],axis = 1,inplace = True)
			df_approved.rename(columns = {'Slab/Fixed/Lumpsum':'pricing_type'}, inplace = True)
			df_approved.rename(columns = {'Distance In Km':'distance_km'}, inplace = True)
			df_approved.rename(columns = {'Conversion Factor':'conv_factor'}, inplace = True)
			df_approved.rename(columns = {'FOV':'fov'}, inplace = True)
			df_approved.rename(columns = {'FSC':'fsc'}, inplace = True)
			df_approved.rename(columns = {'ODA':'oda'}, inplace = True)
			df_approved.rename(columns = {'TAT in Days':'tat'}, inplace = True)
			df_approved.rename(columns = {'UOM From':'from_cap'}, inplace = True)
			df_approved.rename(columns = {'UOM To':'to_cap'}, inplace = True)
			df_approved.rename(columns = {'Docket Charges':'docket_chrgs'}, inplace = True)
			df_approved.rename(columns = {'Handling charges':'handling_chrgs'}, inplace = True)
			df_approved.rename(columns = {'Management fee':'mgmt_fee'}, inplace = True)
			
			df_approved.rename(columns = {'Rate/ Kg':'price_per_kg'}, inplace = True)
			json_load_final_id=json.loads(df_approved_id.to_json(orient='records'))
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['data']=json_load_final
			json_output['ids']=json_load_final_id
			json_output['success']="true"
			json_output['message']="success"
		else:
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_output['success']="false"
			json_output['message']="unsuccessful"
		if len(df_disapproved)>0 and len(df_approved) > 0:
			df_disapproved = df_disapproved.drop_duplicates(subset = ['to_zone','from_zone','warehouse'] ,keep = 'last')
			json_output['success']="true"
			json_output['message']="partial"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
		print(json_output)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"data":[], "success":"false", "message":"Unsuccessful" , "rejectdata":[], "ids":[] }')
	finally:
		cur1.close()
		conn1.close()
	return jsonify(json_output)

@app.route('/ftl_customer_commercial', methods = ['POST'])

# algorithm:
# 1.Read the excel file.
# 2.Validate the file by their column names.--->return if false. continue if true.
# 3.Make all categorical columns into caps and remove all spaces.
# 4.Connect database and fetch-->city_state, vehicle_types, customer list
# 5. Stop if the query returns null for customer, branch and warehouse.
# 6. 
def ftl_customer_commercial():
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		print('Hi 12:53')
		flag = 0 
		content = request.get_json()
		company_id = content['company_id']
		f = request.files["uploadfile"]
		df_drops = pd.read_excel(f.stream,engine = 'openpyxl')
		df_drops.dropna(how = 'all',inplace = True)
		validation_columns = ['branch','warehouse','customer','vehicle_type','from_state','from_city','from_location','to_state','to_city','to_location','rate','distance_km','tat']
				
		check =  all(item in df_drops.columns for item in validation_columns)
		if check is False:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
			
		if len(pd.unique(df_drops['branch'])) > 1 or len(pd.unique(df_drops['warehouse']))  > 1 or len(pd.unique(df_drops['customer'])) > 1 :
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Multiple warehouses/branch/customer defined." , "rejectdata":[],"ids":[] }'))
		print('checking if its converted to df \n',df_drops)
		df_approved = df_drops[0:0]
		df_disapproved = df_drops[0:0]
		df_disapproved['reason'] = ''
		df_drops['contract_type'] = 'rate'
		df_drops['rate_type'] = 'location'
		
		df_drops_copy = df_drops.copy(deep = True)

		datasets = ['to_state','to_city','to_location','from_state','from_city','from_location','customer','warehouse','branch','vehicle_type']
		for columns in datasets:
			df_drops[columns] = df_drops[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.replace(" ","")
			
		query1='''select city,state,location,id from city_state'''
		cur1.execute(query1)
		result = cur1.fetchall()
		result=pd.DataFrame(result,columns = ['city','state','location','id'])
		
		query2='''select id,replace(type,' ','') from vehicle_type'''
		cur1.execute(query2)
		result2 = cur1.fetchall()
		result2=pd.DataFrame(result2,columns = ['vehicle_type_id','vehicle_type'])
		result2["vehicle_type"] = result2["vehicle_type"].str.upper()
		
		for i in df_drops.index:
			query = """select id from ( select id,upper(replace(type,' ',''))as vehicle_type from vehicle_type)a where vehicle_type like '{0}' """.format(df_drops_copy.loc[i,'vehicle_type'])
			cur1.execute(query)
			
			vehicle = cur1.fetchone()
			
			vehicle_id = int(''.join(map(str, vehicle)))
			print('vehicle_id new:',vehicle_id)
			try:
				df_drops.loc[i,'vehicle_type_id'] = int(vehicle_id)
				print("++++++++++++++++++++",df_drops.loc[i,'vehicle_type_id'])
			except:
				answer = 'Entered Vehicle type is incorrect'.format(i+1)
				return jsonify(json.loads('{"data":[], "success":"false", "message":"Entered Vehicle type is incorrect", "rejectdata":[],"ids":[] }'))
		
		query3="""select * from (select id,replace(upper(customer_company),' ','')as customer from customer)a where customer like '{}' and company_id = {}""".format(df_drops_copy.loc[0,'customer'],company_id)
		cur1.execute(query3)
		result3 = cur1.fetchall()
		customer_id_df =pd.DataFrame(result3,columns = ['customer_id','customer'])
		print('hey',df_drops.loc[0,'warehouse'],df_drops.loc[0,'branch'])
		query4="""select * from (select branch.id as branch_id,replace(upper(branch.branch_name),' ','') as branch_name,warehouse.id as warehouse_id, replace(upper(warehouse_name),' ','') as warehouse from branch join warehouse on branch.id = warehouse.branch_id)a where warehouse like '{0}%' and branch_name like '{1}%' """.format(df_drops_copy.loc[0,'warehouse'],df_drops_copy.loc[0,'branch'])
		
		
		cur1.execute(query4)
		result4 = cur1.fetchall()
		print('length of query:',len(result4))
		if len(result4)<= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong Entry of Customer/Branch/Warehouse." , "rejectdata":[],"ids":[] }'))
		
		warehouse_id_df =pd.DataFrame(result4,columns = ['branch_id','branch_name','warehouse_id','warehouse'])
		print(warehouse_id_df)
		
		query5="""SELECT concat(
	replace(upper(customer.customer_company),' ',''),
	',',
	replace(upper(ftl_cust_master.to_state),' ',''),
	',',
	replace(upper(ftl_cust_master.to_city),' ',''),
	',',
	replace(upper(ftl_cust_master.to_location),' ',''),
	',',
	replace(upper(ftl_cust_master.from_state),' ',''),
	',',
	replace(upper(ftl_cust_master.from_city),' ',''),
	',',
	replace(upper(ftl_cust_master.from_location),' ',''),
',',
replace(upper(vehicle_type.type),' ','')) 
FROM "ftl_cust_master" join customer on ftl_cust_master.customer_id = customer.id join vehicle_type on vehicle_type.id = ftl_cust_master.vehicle_type_id where warehouse_id = {0} and customer.company_id = {1}""".format(warehouse_id_df.loc[0,'warehouse_id'],company_id)
		cur1.execute(query5)
		result5 = cur1.fetchall()
		if len(result5) <= 0:
			dup =pd.DataFrame()
			dup['dup'] = ''
		else:
			dup =pd.DataFrame(result5,columns = ['dup'])
		#print(warehouse_id_df)
		
		
		print("checking length \n ",len(result))
		
		l_veh = list(result2['vehicle_type'])
		#print(l_veh)
		dup_list = list(dup['dup'])
		print("entering 1")
			
		datasets2 = ['state','city','location']
		for columns in datasets2:
			result[columns] = result[columns].str.upper()
		print('exception',df_drops[['customer','to_state','to_city','to_location','from_state','from_city','from_location']])
		for i in datasets:
			df_drops_copy[i] = df_drops_copy[i].str.replace(' ','')
		#print('after removin whitespace \n',df_drops[['to_state','to_city','to_location','from_state','from_city','from_location','customer','warehouse']])
		df_drops_copy['dup_check'] = df_drops_copy[['customer','to_state','to_city','to_location','from_state','from_city','from_location','vehicle_type']].agg(','.join, axis=1)
		df_drops_copy["vehicle_type"] = df_drops_copy["vehicle_type"].str.upper()
		df_drops['validation_to_location'] = df_drops[['to_state','to_city','to_location']].agg(','.join, axis=1)
		df_drops['validation_from_location'] = df_drops[['from_state','from_city','from_location']].agg(','.join, axis=1)
		print(result2['vehicle_type'])
		for i in df_drops.index:
			if df_drops_copy.loc[i,'dup_check'] not in dup_list:
				print('no commercial')
				if df_drops.loc[i,'validation_to_location'] != df_drops.loc[i,'validation_from_location']:
					df_drops_copy['vehicle_type'] = df_drops_copy['vehicle_type'].str.replace(' ', '')
					if result2['vehicle_type'].str.contains(df_drops_copy.loc[i,'vehicle_type']).any():
					#if df_drops.loc[i,'vehicle_type'] in l_veh:
						print('there is same vehicle type')

						query_check="""
						SELECT * FROM (select UPPER(REPLACE(state,' ',''))as state, UPPER(REPLACE(city,' ',''))as city,UPPER(REPLACE(location,' ',''))as location from city_state)a where state like '{0}' and city like '{1}' and location like '{2}' """.format(df_drops_copy.loc[i,'to_state'],df_drops_copy.loc[i,'to_city'],df_drops_copy.loc[i,'to_location'])
						cur1.execute(query_check)
						query_check = cur1.fetchall()
						
						query_check2="""SELECT * FROM (select UPPER(REPLACE(state,' ',''))as state, UPPER(REPLACE(city,' ',''))as city,UPPER(REPLACE(location,' ',''))as location from city_state)a where state like '{0}' and city like '{1}' and location like '{2}'
						""".format(df_drops_copy.loc[i,'from_state'],df_drops_copy.loc[i,'from_city'],df_drops_copy.loc[i,'from_location'])
						cur1.execute(query_check2)
						query_check2 = cur1.fetchall()

						
						print(len(query_check))
						if len(query_check) and len(query_check2) > 0:
							print("-----------------------------------------")
							row=df_drops.loc[[i]]
							df_approved = df_approved.append(row)
						else:
							print('wrong location')
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							df_disapproved.loc[i,'reason'] = 'Invalid address entered.'
							flag = 1
					else:
						print('wrong vehicle type')
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
						df_disapproved.loc[i,'reason'] = 'Invalid vehicle_type entered.'
						flag = 1
				else:
					print('same to and from loc')
					row=df_drops.loc[[i]]
					df_disapproved = df_disapproved.append(row)
					df_disapproved.loc[i,'reason'] = 'Same to and from address'
					flag = 1
			else:
				print('already exists')
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'reason'] = 'Commercial already exist.'
				flag = 1
			
		json_output = json.loads('{"data":[], "success":"False", "message":"" , "rejectdata":[], "ids":[] }')
		if len(df_approved) > 0 :
			print('final \n',df_approved)
			df_approved = df_approved.drop_duplicates(subset = ['from_state','to_state','from_city','to_city','from_location','to_location','vehicle_type','customer','warehouse'] ,keep = 'last')
			
			df_approved['customer_id'] = int(customer_id_df.loc[0,'customer_id'])
			print('appended \n',df_approved)
			print('appended 2 \n',warehouse_id_df)
			df_approved['branch_id'] = int(warehouse_id_df.loc[0,'branch_id'])
			df_approved['warehouse_id'] = int(warehouse_id_df.loc[0,'warehouse_id'])
			print('after merge \n',df_approved)
			cols = ['branch_id', 'warehouse_id','customer_id','vehicle_type_id']
			df_approved[cols] = df_approved[cols].applymap(np.int64)
			#df_approved = df_approved.merge(result2,on='vehicle_type')
			df_approved_id = df_approved[['customer_id','branch_id','warehouse_id','rate_type']]
			df_approved = df_approved.drop(['customer_id','branch_id','warehouse_id','rate_type'],axis = 1)
			df_approved_id = df_approved_id[:1]
			#df_approved = df_approved.dropna(axis = 0)
			df_approved.drop(['branch','customer','warehouse','validation_from_location','validation_to_location'],axis = 1,inplace = True)
			df_approved.rename(columns = {'rate':'agreed_price'}, inplace = True)
			json_load_id=json.loads(df_approved_id.to_json(orient='records'))
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['data']=json_load_final
			json_output['ids']=json_load_id
			json_output['success']="true"
			json_output['message']='Success'
		
		else:
			json_output['success']="false"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_output['message']='Unsuccessful'
		if len(df_disapproved) & len(df_approved) > 0:
			json_output['success']="true"
			json_output['message']="partial"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
	
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"data":[], "success":"false", "message":"Unsuccessful" , "rejectdata":[], "ids":[] }')
	finally:
		cur1.close()
		conn1.close()
	return jsonify(json_output)
	
@app.route('/ftl_vendor_commercial', methods = ['POST'])

def ftl_vendor_commercial():
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		print('Hi')
		flag = 0 
		content = request.get_json()

		f = request.files["uploadfile"]
		df_drops = pd.read_excel(f.stream,engine = 'openpyxl')
		df_drops.dropna(how = 'all',inplace = True)
		validation_columns = ['branch','warehouse','vendor','vehicle_type','from_state','from_city','from_location','to_state','to_city','to_location','rate','distance_km','tat']
			
		check =  all(item in df_drops.columns for item in validation_columns)
		if check is False:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
			
		if len(pd.unique(df_drops['branch'])) > 1 or len(pd.unique(df_drops['warehouse']))  > 1 or len(pd.unique(df_drops['vendor'])) > 1 :
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Multiple warehouses/branch/vendor defined." , "rejectdata":[],"ids":[] }'))
		print('checking if its converted to df \n',df_drops)

		#customer_name=content["customer_name"]
		#customer_id=content["customer_id"]
		#branch_name = content["branch_name"]
		#branch_id = content["branch_id"]
		#warehouse_name = content["warehouse_name"]
		#warehouse_id = content["warehouse_id"]
		#way=content["way"]

		#df_drops['customer']= customer_name

		df_approved = df_drops[0:0]
		df_disapproved = df_drops[0:0]
		df_disapproved['reason'] = ''
		df_drops['contract_type'] = 'rate'
		df_drops['rate_type'] = 'location'
		datasets = ['to_state','to_city','to_location','from_state','from_city','from_location','vendor','warehouse','branch']
		df_drops_copy = df_drops.copy(deep = True)
		for columns in datasets:
			df_drops[columns] = df_drops[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.replace(' ','')
			
		query1='''select city,state,location,id from city_state'''
		cur1.execute(query1)
		result = cur1.fetchall()
		result=pd.DataFrame(result,columns = ['city','state','location','id'])
		
		query2='''select id,replace(type,' ','') from vehicle_type'''
		cur1.execute(query2)
		result2 = cur1.fetchall()
		result2=pd.DataFrame(result2,columns = ['vehicle_type_id','vehicle_type'])
		result2["vehicle_type"] = result2["vehicle_type"].str.upper()
		df_drops_copy["vehicle_type"] = df_drops_copy["vehicle_type"].str.upper()
		df_drops_copy["vehicle_type"] = df_drops_copy["vehicle_type"].str.replace(' ', '')
		
		for i in df_drops.index:
			query = """select * from ( select id,upper(replace(type,' ',''))as vehicle_type from vehicle_type)a where vehicle_type like '{0}' """.format(df_drops_copy.loc[i,'vehicle_type'])
			cur1.execute(query)
			print("++++++++++++++++++++",query)
			vehicle_id = cur1.fetchone()
			print('vehicle_id:',vehicle_id)
			try:
				df_drops.loc[i,'vehicle_type_id'] = vehicle_id[0]
			except:
				answer = 'Entered Vehicle type is incorrect'.format(i+1)
				return jsonify(json.loads('{"data":[], "success":"false", "message":"Entered Vehicle type is incorrect", "rejectdata":[],"ids":[] }'))
		
		query3="""select * from (select id,replace(upper(company_name),' ','')as  company_name from carrier_company)a where company_name like '{}' """.format(df_drops_copy.loc[0,'vendor'])
		cur1.execute(query3)
		result3 = cur1.fetchall()
		carrier_id_df =pd.DataFrame(result3,columns = ['carrier_id','vendor'])
		print('hey',df_drops.loc[0,'warehouse'],df_drops.loc[0,'branch'])
		query4="""select * from (select branch.id as branch_id,replace(upper(branch.branch_name),' ','') as branch_name,warehouse.id as warehouse_id, replace(upper(warehouse_name),' ','') as warehouse from branch join warehouse on branch.id = warehouse.branch_id)a where warehouse like '{0}%' and branch_name like '{1}%' """.format(df_drops_copy.loc[0,'warehouse'],df_drops_copy.loc[0,'branch'])
		
		
		cur1.execute(query4)
		result4 = cur1.fetchall()
		if len(result4) <= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong Vendor/Branch/Warehouse entered." , "rejectdata":[],"ids":[] }'))
		warehouse_id_df =pd.DataFrame(result4,columns = ['branch_id','branch_name','warehouse_id','warehouse'])
		print(warehouse_id_df)
		
		print(f)
		query5="""SELECT concat(
replace(upper(carrier_company.company_name),' ',''),
',',
replace(upper(ftl_carr_master.to_state),' ',''),
',',
replace(upper(ftl_carr_master.to_city),' ',''),
',',
replace(upper(ftl_carr_master.to_location),' ',''),
',',
replace(upper(ftl_carr_master.from_state),' ',''),
',',
replace(upper(ftl_carr_master.from_city),' ',''),
',',
replace(upper(ftl_carr_master.from_location),' ',''),
',',
replace(upper(vehicle_type.type),' ','')) 
FROM "ftl_carr_master" join carrier_company on ftl_carr_master.carrier_id = carrier_company.id join vehicle_type on vehicle_type.id = ftl_carr_master.vehicle_type_id where warehouse_id = {0} """.format(warehouse_id_df.loc[0,'warehouse_id'])
		cur1.execute(query5)
		result5 = cur1.fetchall()
		if len(result5) <= 0:
			dup =pd.DataFrame()
			dup['dup'] = ''
		else:
			dup =pd.DataFrame(result5,columns = ['dup'])
		print(warehouse_id_df)
		
		
		print("checking length \n ",len(result))

		l_veh = list(result2['vehicle_type'])
		#print(l_veh)
		dup_list = list(dup['dup'])
		print("entering 1")
			
		datasets2 = ['state','city','location']
		for columns in datasets2:
			result[columns] = result[columns].str.upper()
		print('exception',df_drops[['vendor','to_state','to_city','to_location','from_state','from_city','from_location']])
		for i in datasets:
			df_drops_copy[i] = df_drops_copy[i].str.replace(' ','')
		print('after removin whitespace \n',df_drops[['to_state','to_city','to_location','from_state','from_city','from_location','vendor','warehouse']])
		df_drops_copy['dup_check'] = df_drops_copy[['vendor','to_state','to_city','to_location','from_state','from_city','from_location','vehicle_type']].agg(','.join, axis=1)
		df_drops['validation_to_location'] = df_drops[['to_state','to_city','to_location']].agg(','.join, axis=1)
		df_drops['validation_from_location'] = df_drops[['from_state','from_city','from_location']].agg(','.join, axis=1)
		#print(result[['state','city','location']])
		print('1')
		#l = list(result[['state','city','location']].fillna('').agg(','.join, axis=1))
		#df22 = pd.DataFrame(l)
		#df22.to_csv('list.csv', index=False)
		#print(l[0])
		print('2')
		for i in df_drops.index:
			if df_drops_copy.loc[i,'dup_check'] not in dup_list:
				print('no commercial')
				#print(df_drops.loc[i,'validation_to_location'],df_drops.loc[i,'validation_from_location'])
				if df_drops.loc[i,'validation_to_location'] != df_drops.loc[i,'validation_from_location']:
					if df_drops_copy.loc[i,'vehicle_type'] in l_veh:
						print('there is same vehicle type')

						query_check="""
						SELECT * FROM (select UPPER(REPLACE(state,' ',''))as state, UPPER(REPLACE(city,' ',''))as city,UPPER(REPLACE(location,' ',''))as location from city_state)a where state like '{0}' and city like '{1}' and location like '{2}' """.format(df_drops_copy.loc[i,'to_state'],df_drops_copy.loc[i,'to_city'],df_drops_copy.loc[i,'to_location'])
						cur1.execute(query_check)
						query_check = cur1.fetchall()
						
						query_check2="""SELECT * FROM (select UPPER(REPLACE(state,' ',''))as state, UPPER(REPLACE(city,' ',''))as city,UPPER(REPLACE(location,' ',''))as location from city_state)a where state like '{0}' and city like '{1}' and location like '{2}'
						""".format(df_drops_copy.loc[i,'from_state'],df_drops_copy.loc[i,'from_city'],df_drops_copy.loc[i,'from_location'])
						cur1.execute(query_check2)
						query_check2 = cur1.fetchall()

						
						print(len(query_check))
						if len(query_check) and len(query_check2) > 0:
							print("-----------------------------------------")
							row=df_drops.loc[[i]]
							df_approved = df_approved.append(row)
						else:
							print('wrong location')
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							df_disapproved.loc[i,'reason'] = 'Invalid address entered.'
							flag = 1
					else:
						print('wrong vehicle type')
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
						df_disapproved.loc[i,'reason'] = 'Invalid vehicle type entered.'
						flag = 1
				else:
					print('same to and from loc')
					row=df_drops.loc[[i]]
					df_disapproved = df_disapproved.append(row)
					df_disapproved.loc[i,'reason'] = 'Source and destination is same.'
					flag = 1
			else:
				print('already exists')
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'reason'] = 'Commercial already exist.'
				flag = 1
			
		json_output = json.loads('{"data":[], "success":"False", "message":"" , "rejectdata":[], "ids":[] }')
		if len(df_approved) > 0 :
			print('final \n',df_approved)
			df_approved = df_approved.drop_duplicates(subset = ['from_state','to_state','from_city','to_city','from_location','to_location','vehicle_type','vendor','warehouse'] ,keep = 'last')
			
			df_approved['carrier_id'] = carrier_id_df.loc[0,'carrier_id']
			print('appended \n',df_approved)
			print('appended 2 \n',warehouse_id_df)
			df_approved['warehouse_id'] = warehouse_id_df.loc[0,'warehouse_id']
			df_approved['branch_id'] = warehouse_id_df.loc[0,'branch_id']
			
			#df_approved = df_approved.merge(result2,on='vehicle_type')
			df_approved_id = df_approved[['carrier_id','branch_id','warehouse_id','rate_type']]
			df_approved = df_approved.drop(['carrier_id','branch_id','warehouse_id','rate_type'],axis = 1)
			df_approved_id = df_approved_id[:1]
			#df_approved = df_approved.dropna(axis = 0)
			df_approved.drop(['branch','vendor','warehouse','validation_from_location','validation_to_location'],axis = 1,inplace = True)
			df_approved.rename(columns = {'rate':'agreed_price'}, inplace = True)
			json_load_id=json.loads(df_approved_id.to_json(orient='records'))
			print('after merge \n',df_approved)
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['data']=json_load_final
			json_output['ids']=json_load_id
			json_output['success']="true"
			json_output['message']='Success'
		
		else:
			json_output['success']="false"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_output['message']='Unsuccessful'
		if len(df_disapproved) & len(df_approved) > 0:
			json_output['success']="true"
			json_output['message']="partial"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
		
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"data":[], "success":"false", "message":"Unsuccessful" , "rejectdata":[], "ids":[] }')
	finally:
		cur1.close()
		conn1.close()
	return jsonify(json_output)

@app.route('/sub_customer_bulk_creation', methods = ['POST'])

def sub_customer_bulk_creation():
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		print("hi")
		current_date = datetime.date.today()
		f = request.files["uploadfile"]
		content = request.form
		customer = content['customer_name']
		print("customer:",customer)
		customer_id = content['customer_id']
		df_drops = pd.read_excel(f.stream,engine = 'openpyxl')
		df_drops.dropna(how = 'all',inplace = True)
		df_drops["customer"] = customer
		df_drops["customer_id"] = customer_id
		validation_columns = ['Sub Customer Code*','Code Created Date','Sub Customer Name*','Address Name*','Phone No*','Email ID*','GST No*','PAN*','Address Line 1*','Address Line 2*','State*','City*','Location*','Pincode*','Product Category*']
		check =  all(item in df_drops.columns for item in validation_columns)
		if check is False:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[] }'))
		
		df_drops.dropna(subset=['Address Name*','Sub Customer Code*'],inplace = True)
		
		df_drops[validation_columns] = df_drops[validation_columns].applymap(str)
		df_drops['customer_id'] = df_drops['customer_id'].map(np.int64)
		df_drops.columns= df_drops.columns.str.lower()
		df_drops.columns= df_drops.columns.str.replace('*',"")
		df_drops.dropna(how = 'all',inplace = True)
		df_drops_copy = df_drops.copy(deep = True)
		df_approved = df_drops[0:0]
		df_disapproved = df_drops[0:0]
		cat_var = [key for key in dict(df_drops.dtypes)if dict(df_drops.dtypes)[key] in ['object'] ]
		for columns in cat_var:
			#print(columns)
			df_drops_copy[columns] = df_drops_copy[columns].str.upper()
			df_drops[columns] = df_drops[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.replace(' ','')
			
		
		
		check_unique_address_name = len(set(df_drops_copy['address name'])) == len(df_drops_copy['address name'])
		
		#cc = pd.Series(df_drops_copy['address name']).is_unique
		#print(cc)
		
		if check_unique_address_name is False:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Column Address Name is not unique." , "rejectdata":[] }'))
		
		check_unique_customer_code = len(set(df_drops_copy['sub customer code'])) == len(df_drops_copy['sub customer code'])
		
		if check_unique_customer_code is False:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Column sub customer code is not unique." , "rejectdata":[]}'))

			
		for i in df_drops_copy.index:
			#try:
			df_drops_copy['pincode'] = df_drops_copy['pincode'].astype('int64')
			# except:
				# print("except for is integer")
				# row=df_drops.loc[[i]]
				# df_disapproved = df_disapproved.append(row)
				# df_disapproved.loc[i,'reason'] = 'Invalid pincode'
			check_pincode = len(str(df_drops_copy.loc[i,'pincode'])) == 6 and isinstance(df_drops_copy.loc[i,'pincode'],int)
			#print("### CHECKIN PINCODE CONDITION #####",check_pincode)

				
			query_check="""
						SELECT * FROM (select UPPER(REPLACE(state,' ',''))as state, UPPER(REPLACE(city,' ',''))as city,UPPER(REPLACE(location,' ',''))as location from city_state)a where state like '{0}' and city like '{1}' and location like '{2}' """.format(df_drops_copy.loc[i,'state'],df_drops_copy.loc[i,'city'],df_drops_copy.loc[i,'location'])
			cur1.execute(query_check)
			query_check = cur1.fetchall()
			
			unique_address = """ SELECT * FROM (select UPPER(REPLACE(address_name,' ',''))as address_name from customeraddress where customer_id={})a where address_name like '{}'
			""".format(df_drops_copy.loc[0,'customer_id'],df_drops_copy.loc[i,'address name'])
			cur1.execute(unique_address)
			unique_address = cur1.fetchall()
			
			
			unique_cc = """ SELECT * FROM (select UPPER(REPLACE(contact_code,' ',''))as contact_code from customeraddress where customer_id={})a where contact_code like '{}'
			""".format(df_drops_copy.loc[0,'customer_id'],df_drops_copy.loc[i,'sub customer code'])
			cur1.execute(unique_cc)
			unique_cc = cur1.fetchall()
			
			
			material_check=""" select * from (select REPLACE(UPPER(material),' ','') as material_type from material_type)a where material_type like '{}' """.format(df_drops_copy.loc[i,'product category'])
			cur1.execute(material_check)
			material_check = cur1.fetchone()

				#print(len(query_check))
			if query_check is None :
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'reason'] = 'Invalid address entered.'
				df_disapproved.loc[i,'Code'] = 'E1'
				
			elif len(unique_address) > 0 :
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'reason'] = 'Address Name already exists.'
				df_disapproved.loc[i,'Code'] = 'E2'
			
			elif len(unique_cc) > 0 :
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'reason'] = 'Sub customer code already exists.'
				df_disapproved.loc[i,'Code'] = 'E3'
				
			elif material_check is None :
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'reason'] = 'Invalid Product Category entered.'
				df_disapproved.loc[i,'Code'] = 'E4'
			
			elif check_pincode is False :
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'reason'] = 'Invalid pincode entered.'
				df_disapproved.loc[i,'Code'] = 'E5'

			else:
				row=df_drops.loc[[i]]
				df_approved = df_approved.append(row)
		
		json_output = json.loads('{"data":[], "success":"False", "message":"" , "rejectdata":[],"error_counts":[]}')
		
		df_approved.rename(columns = {'address line 1':'address_line_one'}, inplace = True)
		df_approved.rename(columns = {'address line 2':'address_line_two'}, inplace = True)
		df_approved.rename(columns = {'code created date':'contact_code_created_date'}, inplace = True)
		df_approved.rename(columns = {'sub customer code':'contact_code'}, inplace = True)
		df_approved.rename(columns = {'sub customer name':'contact_name'}, inplace = True)
		df_approved.rename(columns = {'product category':'product_category'}, inplace = True)
		df_approved.rename(columns = {'phone no':'contact_number'}, inplace = True)
		df_approved.rename(columns = {'address name':'address_name'}, inplace = True)
		df_approved.rename(columns = {'gst no':'gst_number'}, inplace = True)
		df_approved.rename(columns = {'pan':'pan_number'}, inplace = True)
		df_approved.rename(columns = {'email id':'contact_email'}, inplace = True)
		df_approved['contact_code_created_date'] = pd.to_datetime(df_approved['contact_code_created_date']).dt.strftime('%Y-%m-%d')
		df_approved['contact_code_created_date']=df_approved['contact_code_created_date'].astype(str)
		df_approved['pan_number']=df_approved['pan_number'].astype(str)
		df_approved['pincode']=df_approved['pincode'].apply(np.float64)
		df_approved['pincode']=df_approved['pincode'].apply(np.int64)
		df_approved['pincode']=df_approved['pincode'].astype(str)
		df_approved['gst_number']=df_approved['gst_number'].astype(str)
		
		for i in df_approved.index:
			if df_approved.loc[i,'contact_code_created_date'] == 'NaT':
				df_approved.loc[i,'contact_code_created_date'] = current_date
		df_approved['address_line_two'] = ''
		
		if len(df_approved) > 0 and len(df_disapproved) == 0:
			print('final: \n',df_approved)
			
			df_approved['contact_code_created_date'] = pd.to_datetime(df_approved['contact_code_created_date']).dt.strftime('%Y/%m/%d %H:%M:%S')
			df_approved['contact_code_created_date']=df_approved['contact_code_created_date'].astype(str)
			
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['data']=json_load_final
			json_output['success']="true"
			json_output['message']="Success"
		elif len(df_approved) == 0 and len(df_disapproved) > 0:
			e1 = str((df_disapproved['Code'] == 'E1').sum())
			e2 = (df_disapproved['Code'] == 'E2').sum()
			e3 = (df_disapproved['Code'] == 'E3').sum()
			e4 = (df_disapproved['Code'] == 'E4').sum()
			json_output['success']="false"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_output['message']='Unsuccessful'
			# a = {"E1":[e1],"E2":[e2],"E3":[e3],"E4":[e4]}
			# a = json.dumps(a, indent = 4)
			# json_output['error_counts']= a
		else:
			df_approved['contact_code_created_date'] = pd.to_datetime(df_approved['contact_code_created_date']).dt.strftime('%Y/%m/%d %H:%M:%S')
			df_approved['contact_code_created_date']=df_approved['contact_code_created_date'].astype(str)
			# e1 = str((df_disapproved['Code'] == 'E1').sum())
			# e2 = (df_disapproved['Code'] == 'E2').sum()
			# e3 = (df_disapproved['Code'] == 'E3').sum()
			# e4 = (df_disapproved['Code'] == 'E4').sum()
			json_output['success']="true"
			json_output['message']="partial"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['data']=json_load_final
			# a = {"E1":[e1],"E2":[e2],"E3":[e3],"E4":[e4]}
			# a = json.dumps(a, indent = 4)
			# json_output['error_counts']= a
			#json_output['error_counts']={"E1":e1[0],"E2":e2[0],"E3":e3[0],"E4":e4[0]}

	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"data":[], "success":"false", "message":"Unsuccessful" , "rejectdata":[], "ids":[] }')
	finally:
		cur1.close()
		conn1.close()
		
	return jsonify(json_output)

@app.route('/ltl_bulkbooking',methods=['GET','POST'])

def ltl_bulkbooking():
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		print('Hi')

		f = request.files["uploadfile"]
		content = request.form
		#print(a['branch_name'])
		#content = json.loads(a)
		
		branch = content['branch_name']
		print(branch)
		customer = content['customer_name']
		vendor = content['vendor_name']
		warehouse = content['warehouse_name']

		branch_id = content['branch_id']
		customer_id  = content['customer_id']
		carrier_id  = content['carrier_id']
		warehouse_id  = content['warehouse_id']
		pickup_address_id  = content['pickup_address_id']
		pickup_address = content['pickup_address']

		customer_from_state  = content['customer_from_state']
		customer_from_addressname  = content['customer_from_addressname']
		customer_from_city  = content['customer_from_city']
		#vendor_from_state  = content['vendor_from_state']

		df = pd.read_excel(f.stream,engine = 'openpyxl')
		df.dropna(how = 'all',inplace = True)
		validation_columns = ['drop_number','Destination/Consignee','to_state','to_city','to_location','Material Type','LR Number','SKU Code','Carton Code','Length(mm)','Breadth(mm)','Height(mm)','Actual Weight(kg)','No of Box','Customer Loading Charge','Customer Unloading Charge','Customer Other Charge','Surcharges','Cover Charges','Cover Collection Charges','Door Collection Charges','Door Delivery Charges','Value Added services','Statistical Charges','Misc Charges','Vendor Loading Charge','Vendor Unloading Charge','Vendor Other Charge','Invoice Number','Invoice Value','Invoice Date','Ewaybill Number','Ewaybill valid From Date','Ewaybill valid To Date','Vendor LR Number','Vendor LR Date']
			
		check =  all(item in df.columns for item in validation_columns)
		if check is False:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong File uploaded." , "rejectdata":[],"ids":[] }'))
		unique_list = df['drop_number'].unique()
		final_list = []
		
		df['dimensions']=''
		df_approved = df[0:0]
		df_disapproved = df[0:0]
		df_disapproved["Reason"] = ""
		for i in unique_list:
			print('**********************************',i)
			df1 = df[df['drop_number']==i]
			df1 = df1.dropna(how = 'all')
			box_validation = 0
			for each_drop in df1.index:
				print(df1.loc[each_drop,'Destination/Consignee'])
				if df1.loc[each_drop,'Destination/Consignee'] is not np.nan or df1.loc[each_drop,'to_state'] is not np.nan and df1.loc[each_drop,'to_city'] is not np.nan and df1.loc[each_drop,'to_location'] is not np.nan:
					print("entering if")
					box = df1.loc[each_drop,'No of Box']
					storing_index = each_drop
				else:
					print("entering else")
					box_validation = box_validation + df1.loc[each_drop,'No of Box']
					inner_dimensions = {"Length(mm)":df1.loc[each_drop,'Length(mm)'],"Breadth(mm)":df1.loc[each_drop,'Breadth(mm)'],"Height(mm)":df1.loc[each_drop,'Height(mm)'],"box_number":df1.loc[each_drop,'No of Box']}
					print(inner_dimensions)
					final_list.append(inner_dimensions)
			df.at[storing_index,'dimensions'] = final_list
			print("box_validation,box",box_validation,box)
			final_list = []
			df['total_no_of_items'] = box
			if box_validation != box:
				#return jsonify(json.loads('{"data":[], "success":"false", "message":"Summation of Number of packages not satisfying." , "rejectdata":[],"ids":[] }'))
				print('wrong packages count')
				row=df.loc[[storing_index]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'Reason'] = 'Summation of Number of packages not satisfying.'
				df.drop(df.index[[storing_index]],inplace = True)
				df.reset_index(drop=True,inplace=True)
			else:
				print('correct packages count')
				row=df.loc[[storing_index]]
				df_approved = df_approved.append(row)
			
		df_drops = df_approved
		
			
		df_drops = df_drops.dropna(axis =0,subset = ['Destination/Consignee'])
		print("************************************************************** \,",df_drops)
		df_drops.reset_index(drop = True,inplace=True)
		print(df_drops[['drop_number','dimensions']])
		if len(df_drops) <= 0:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Uploaded excel sheet is empty." , "rejectdata":[],"ids":[] }'))

		df_drops['branch'] = branch
		df_drops['customer'] = customer
		df_drops['vendor'] = vendor
		df_drops['warehouse'] = warehouse
		df_drops['pickup_address'] = pickup_address

		df_drops['branch_id'] = branch_id
		df_drops['customer_id'] = customer_id
		df_drops['carrier_id'] = carrier_id
		df_drops['warehouse_id'] = warehouse_id
		df_drops['pickup_address_id'] = pickup_address_id

		df_drops['customer_from_state'] = customer_from_state
		df_drops['customer_from_addressname'] = customer_from_addressname
		df_drops['customer_from_city'] = customer_from_city
		#df_drops['vendor_from_state'] = vendor_from_state
		df_drops['drop_address_id'] = 0
		df_drops['destination_code'] = ''
		if len(pd.unique(df_drops['branch'])) > 1 or len(pd.unique(df_drops['warehouse']))  > 1 or len(pd.unique(df_drops['customer'])) > 1 :
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Multiple warehouses/branch/customer defined." , "rejectdata":[],"ids":[] }'))
		print(customer,vendor,warehouse,branch_id,customer_id,carrier_id,warehouse_id,pickup_address_id,pickup_address,customer_from_state,customer_from_addressname,customer_from_city)

		print('checking if its converted to df \n',df_drops)
		
		print("############################1##############################")
		datasets = ['to_state','to_city','to_location','customer_from_state','customer_from_city','customer_from_addressname','vendor','warehouse','branch','customer','vendor','Material Type','Destination/Consignee']
		
		#print(branch,customer,warehouse)

		for j in df_drops.index:
			if len(df_drops.loc[j,'to_state']) and len(df_drops.loc[j,'to_city']) and len(df_drops.loc[j,'to_location']) and len(df_drops.loc[j,'Material Type']) <= 0:
				df_drops = df_drops.drop(j)
				print("dropped because mand fields not entered")
				
		# validate if duplicate LR numbers is entered
		dup_lr = df_drops.duplicated(subset=['LR Number']).any()
		#print(dup_lr, end='\n\n') # True
		if dup_lr is True:
			return jsonify(json.loads('{"data":[], "success":"false", "message":"Duplicate LR numbers entered in excel sheet." , "rejectdata":[],"ids":[] }'))
			df_disapproved = df_drops
			df_drops = df_drops[0:0]
		df_drops_copy = df_drops.copy(deep = True)
		for columns in datasets:
			#print(columns)
			df_drops_copy[columns] = df_drops_copy[columns].str.upper()
			df_drops[columns] = df_drops[columns].str.upper()
			df_drops_copy[columns] = df_drops_copy[columns].str.replace(' ','')
			
		# for i in df_drops.index:
			
			# query = """ select * from (select id,contact_code,upper(replace(address,' ','')) as address from customeraddress where customer_id = {0})a where address like '{1}' """.format(df_drops.loc[i,'customer_id'],df_drops_copy.loc[i,'Destination/Consignee'])
			# cur1.execute(query)
			# print("++++++++++++++++++++",query)
			# drop_address_id = cur1.fetchone()
			# print('drop_address_id:',drop_address_id)
			# try:
				# df_drops.loc[i,'drop_address_id'] = drop_address_id[0]
				# df_drops.loc[i,'destination_code'] = drop_address_id[1]
			# except:
				# answer = 'Entered location in the row number {0} is invalid.'.format(i+1)
				# row=df_drops.loc[[i]]
				# df_disapproved = df_disapproved.append(row)
				# # df_drops.drop([i],inplace = True)
				# # df_drops.reset_index(drop=True,inplace = True)
				# # df_drops_copy.drop([i],inplace = True)
				# # df_drops_copy.reset_index(drop=True,inplace = True)
				# messages = "Entered location in the row number {} is invalid.".format(i+2)
				# #json_output = json.loads('{"data":[], "success":"false", "message":"" , "rejectdata":[],"ids":[] }')
				# #json_output["message"] = messages
				# #return jsonify(json_output)
		# #print('Entered Destination Address is invalid')
		# #row=df_drops.loc[[i]]
		# #df_disapproved = df_disapproved.append(row)
		
			# query7=""" select * from (select id,REPLACE(UPPER(material),' ','') as material_type from material_type)a where material_type like '{0}%' """.format(df_drops_copy.loc[i,'Material Type'])
			# cur1.execute(query7)
			# result7 = cur1.fetchone()
			# try:
				# df_drops.loc[i,'material_type_id'] = result7[0]
				# print("material type id==>",df_drops.loc[i,'material_type_id'])
			# except:
				# answer1 = 'Entered Material in the row number {0} is invalid.'.format(i+1)
				# row=df_drops.loc[[i]]
				# df_disapproved = df_disapproved.append(row)	
				# # df_drops.drop([i],inplace = True)
				# # df_drops.reset_index(drop=True,inplace = True)
				# # df_drops_copy.drop([i],inplace = True)
				# # df_drops_copy.reset_index(drop=True,inplace = True)
				# #return jsonify(json.loads('{"data":[], "success":"false", "message":"Entered Material is invalid." , "rejectdata":[],"ids":[] }'))
		
		# query3="""select REPLACE(UPPER(ltl_cust_master.to_state),' ',''),REPLACE(UPPER(ltl_cust_master.to_city),' ',''),UPPER(ltl_cust_master.to_location),REPLACE(UPPER(ltl_cust_master.from_state),' ',''),REPLACE(UPPER(ltl_cust_master.from_city),' ',''),UPPER(ltl_cust_master.from_location),REPLACE(UPPER(ltl_cust_master.from_zone),' ',''),REPLACE(UPPER(ltl_cust_master.to_zone),' ','') from ltl_cust_master where customer_id = {0} and branch_id = {1} and warehouse_id = {2} and status = 'Active' """.format(df_drops.loc[0,'customer_id'],df_drops.loc[0,'branch_id'],df_drops.loc[0,'warehouse_id'])
		# cur1.execute(query3)
		# result3 = cur1.fetchall()
		# cust_commercial_check=pd.DataFrame(result3,columns = ['to_state','to_city','to_location','from_state','from_city','from_location','from_zone','to_zone'])

		# query4="""select REPLACE(UPPER(ltl_carr_master.to_state),' ',''),REPLACE(UPPER(ltl_carr_master.to_city),' ',''),UPPER(ltl_carr_master.to_location),REPLACE(UPPER(ltl_carr_master.from_state),' ',''),REPLACE(UPPER(ltl_carr_master.from_city),' ',''),UPPER(ltl_carr_master.from_location),REPLACE(UPPER(ltl_carr_master.from_zone),' ',''),REPLACE(UPPER(ltl_carr_master.to_zone),' ','') from ltl_carr_master where carrier_id = {0} and branch_id = {1} and warehouse_id = {2} and status = 'Active' """.format(df_drops.loc[0,'carrier_id'],df_drops.loc[0,'branch_id'],df_drops.loc[0,'warehouse_id'])
		# cur1.execute(query4)
		# result4 = cur1.fetchall()
		# carr_commercial_check=pd.DataFrame(result3,columns = ['to_state','to_city','to_location','from_state','from_city','from_location','from_zone','to_zone'])
		
		# query5='''select replace(UPPER(zone),' ',''),replace(UPPER(state),' ','') from customer_zone where customer_id = {0}'''.format(df_drops.loc[0,'customer_id'])
		# cur1.execute(query5)
		# result5 = cur1.fetchall()
		# if len(result5) <= 0:
			# return jsonify(json.loads('{"data":[], "success":"false", "message":"Wrong Entry of Customer." , "rejectdata":[],"ids":[] }'))
		# else:
			# customer_zone = pd.DataFrame(result5,columns = ['zone','state'])
		
		# query6='''select replace(UPPER(zone),' ',''),replace(UPPER(state),' ','') from carrier_zone where carrier_id = {0}'''.format(df_drops.loc[0,'carrier_id'])
		# cur1.execute(query6)
		# result6 = cur1.fetchall()
		# carrier_zone = pd.DataFrame(result6,columns = ['zone','state'])
		print("#########################checkpoint 2############################")
		query7="""SELECT concat(upper(customer.customer_company),',',upper(ltl_cust_master.to_state),',',upper(ltl_cust_master.to_city),',',upper(ltl_cust_master.to_location),',',upper(ltl_cust_master.from_state),',',upper(ltl_cust_master.from_city),',',upper(ltl_cust_master.from_location)) FROM "ltl_cust_master" join customer on ltl_cust_master.customer_id = customer.id; """
		cur1.execute(query7)

		result7 = cur1.fetchall()
		dup =pd.DataFrame(result7,columns = ['dup'])
		

			
		# the code might blot 
		#customer_lr_numbers_list = list(customer_lr_numbers['customer_lr_number'])
		#material_id_df_list = list(material_id_df['material_type'])
		df_approved  = df_approved[0:0]
		for i in df_drops.index:
			#entering for location
			#if rate_type == 'location':
			#cust_commercial_check_location_list = list(cust_commercial_check_location['dup'])

			
			query1='''select id,REPLACE(UPPER(material),' ','') from material_type'''
			cur1.execute(query1)
			result1 = cur1.fetchall()
			material_id_df=pd.DataFrame(result1,columns = ['material_type_id','material_type'])
			print("**************customer_id*************",df_drops.loc[0,'customer_id'])
			query2='''select REPLACE(UPPER(customer_lr_number),' ','') from customer_lr_numbers where status = 'Used' and customer_id = {}'''.format(df_drops.loc[0,'customer_id'])
			print(query2)
			cur1.execute(query2)
			result2 = cur1.fetchall()
			if len(result2) <= 0:
				customer_lr_numbers = pd.DataFrame()
				customer_lr_numbers['customer_lr_number']=''
			else:
				customer_lr_numbers = pd.DataFrame(result2,columns = ['customer_lr_number'])

			
			query = """ select * from (select id,contact_code,upper(replace(address,' ','')) as address from customeraddress where customer_id = {0})a where address like '{1}' """.format(df_drops.loc[i,'customer_id'],df_drops_copy.loc[i,'Destination/Consignee'])
			cur1.execute(query)
			print("++++++++++++++++++++",query)
			drop_address_id = cur1.fetchone()
			print('drop_address_id:',drop_address_id)
			try:
				print(drop_address_id[0])
				df_drops.loc[i,'drop_address_id'] = drop_address_id[0]
				df_drops.loc[i,'destination_code'] = drop_address_id[1]
			except:
				print("entering exception for no drop_address_id")
				answer = 'Entered location in the row number {0} is invalid.'.format(i+1)
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'Reason'] = 'Entered location is invalid.'
			
			query7=""" select * from (select id,REPLACE(UPPER(material),' ','') as material_type from material_type)a where material_type like '{0}%' """.format(df_drops_copy.loc[i,'Material Type'])
			cur1.execute(query7)
			result7 = cur1.fetchone()
			try:
				df_drops.loc[i,'material_type_id'] = result7[0]
				print("material type id==>",df_drops.loc[i,'material_type_id'])
			except:
				answer1 = 'Entered Material in the row number {0} is invalid.'.format(i+1)
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)	
				df_disapproved.loc[i,'Reason']='Entered Material is invalid.'
			
			
			query_check="""
			SELECT * FROM (select UPPER(REPLACE(state,' ',''))as state, UPPER(REPLACE(city,' ',''))as city,UPPER(REPLACE(location,' ',''))as location from city_state)a where state like '{}%' and city like '{}%' and location like '{}%' """.format(df_drops_copy.loc[i,'to_state'],df_drops_copy.loc[i,'to_city'],df_drops_copy.loc[i,'to_location'])
			cur1.execute(query_check)
			query_check = cur1.fetchall()
			
			#df_drops_copy = df_drops.copy(deep = True)
			#for i in datasets:
			#	df_drops_copy[i] = df_drops_copy[i].str.replace(' ','')
			df_drops['LR Number'] = df_drops['LR Number'].astype(str)
			if len(query_check) > 0:
				print('entered address is right')
				#df_drops['dup_check'] = df_drops_copy[['customer','to_state','to_city','to_location','customer_from_state','customer_from_city','customer_from_addressname']].agg(','.join, axis=1)
				print("printing lr number column: \n",df_drops.loc[i,'LR Number'],type(df_drops.loc[i,'LR Number']))
				if df_drops.loc[i,'LR Number'] is None or df_drops.loc[i,'LR Number']=='nan':
					print("entering for lr being null")
					df_drops.loc[i,'LR Number'] = ""
				if ( df_drops.loc[i,'LR Number'] not in customer_lr_numbers['customer_lr_number']):
					df_drops['Material Type'] = df_drops_copy['Material Type'].str.replace(' ','')
					#print(df_drops.loc[i,'Material Type'],material_id_df['material_type'])
					if material_id_df['material_type'].str.contains(df_drops.loc[i,'Material Type']).any():
						if df_drops.loc[i,'drop_address_id'] is not None and df_drops.loc[i,'material_type_id'] is not None:
							row=df_drops.loc[[i]]
							df_approved = df_approved.append(row)
						
						# if 'from_zone' not in df_drops.columns:
							# dup_list = list(dup['dup'])
							# df_drops['dup_check'] = df_drops[['customer','to_state','to_city','to_location','from_state','from_city','from_location']].agg(','.join, axis=1)
							# if df_drops.loc[i,'dup_check'] not in dup_list:
								
								# row=df_drops.loc[[i]]
								# df_approved = df_approved.append(row)
							
								
						# if cust_commercial_check['from_state'].str.contains(df_drops.loc[i,'from_state']).any() and  cust_commercial_check['from_city'].str.contains(df_drops.loc[i,'from_city']).any() and cust_commercial_check['from_location'].str.contains(df_drops.loc[i,'from_location']).any() and cust_commercial_check['to_state'].str.contains(df_drops.loc[i,'Destination/Consignee']).any() and  cust_commercial_check['to_city'].str.contains(df_drops.loc[i,'Destination/Consignee']).any() and cust_commercial_check['to_location'].str.contains(df_drops.loc[i,'Destination/Consignee']).any() :
							# if carr_commercial_check['from_state'].str.contains(df_drops.loc[i,'from_state']).any() and  carr_commercial_check['from_city'].str.contains(df_drops.loc[i,'from_city']).any() and carr_commercial_check['from_location'].str.contains(df_drops.loc[i,'from_location']).any() and carr_commercial_check['to_state'].str.contains(df_drops.loc[i,'Destination/Consignee']).any() and  carr_commercial_check['to_city'].str.contains(df_drops.loc[i,'Destination/Consignee']).any() and carr_commercial_check['to_location'].str.contains(df_drops.loc[i,'Destination/Consignee']).any() :
								
							# else:
								# print('No commercial defined for vendor')
								# row=df_drops.loc[[i]]
								# df_disapproved = df_disapproved.append(row)
						else:
							print('Wrong entry of Destination/Wrong entry of material type.')
							row=df_drops.loc[[i]]
							df_disapproved = df_disapproved.append(row)
							df_disapproved.loc[i,'Reason']='Wrong entry of Destination/Wrong entry of material type.'
						# df1 = cust_commercial_check_location[(cust_commercial_check_location['from_state']== df_drops.loc[i,'customer_from_state']) &  cust_commercial_check_location['from_city']== df_drops.loc[i,'customer_from_city'])]
						
						# if len(df1) > 1:
						# #if df_drops.loc[i,'customer_from_state'] and df_drops.loc[i,'customer_from_city'] in cust_commercial_check_location_list :
						
					# #print('entering when the data enters if condition')
					# #if df_drops.loc[i,'lr_number'] not in customer_lr_number_list:
					# #	if df_drops.loc[i,'dup_check'] in dup_list:
					
						# else:
							# print('entering when the data not enters if condition')
							# row=df_drops.loc[[i]]
							# df_disapproved = df_disapproved.append(row)
					else:
						print('Material type')
						row=df_drops.loc[[i]]
						df_disapproved = df_disapproved.append(row)
						df_disapproved.loc[i,'Reason']='Wrong entry of material type.'
				else:
					print('incorrect LR number')
					row=df_drops.loc[[i]]
					df_disapproved = df_disapproved.append(row)
					df_disapproved.loc[i,'Reason']='LR number already in use.'
			else:
				print('to location incorrect')
				row=df_drops.loc[[i]]
				df_disapproved = df_disapproved.append(row)
				df_disapproved.loc[i,'Reason']='LR number already in use.'

			
			
			# else:
				# dup_list = list(dup_zone['dup'])
				# df_drops['dup_check'] = df_drops[['customer','from_zone','to_zone']].agg(','.join, axis=1)
				# conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
				# cur1 = conn1.cursor()
				# query_check="""
				# SELECT * FROM (select upper(zone)as zone from customer_zone)a where zone like '{0}%' and customer_id = {1} """.format(df_drops.loc[i,'from_zone'],customer_id_df[0,'customer_id'])
				# cur1.execute(query_check)
				# query_check = cur1.fetchall()
				
				# query_check2="""
				# SELECT * FROM (select upper(zone)as zone from customer_zone)a where zone like '{0}%' and customer_id = {1} """.format(df_drops.loc[i,'to_zone'],customer_id_df[0,'customer_id'])
				# cur1.execute(query_check2)
				# query_check2 = cur1.fetchall()
				
				# cur1.close()
				# conn1.close()
				# if len(query_check) and len(query_check2) > 0:
					# if ( df_drops.loc[i,'lr_number'] not in customer_lr_number_list and df_drops.loc[i,'dup_check'] in dup_list and df_drops.loc[i,'material_type'] in material_id_df_list):
						# print('entering when the data enters if condition')
						# #if df_drops.loc[i,'lr_number'] not in customer_lr_number_list:
						# #	if df_drops.loc[i,'dup_check'] in dup_list:
						# row=df_drops.loc[[i]]
						# df_approved = df_approved.append(row)
					# else:
						# print('entering when the data not enters if condition')
						# row=df_drops.loc[[i]]
						# df_disapproved = df_disapproved.append(row)
				# else:
					# print('entering when the data not enters if condition')
					# row=df_drops.loc[[i]]
					# df_disapproved = df_disapproved.append(row)
		
		json_output = json.loads('{"drops":[], "success":"", "message":"" , "rejectdata":[] }')
		if len(df_approved) > 0 :
			# df_approved = df_approved.merge(customer_id_df,on='customer')
			# df_approved = df_approved.merge(warehouse_id_df,on='warehouse')
			# df_approved = df_approved.merge(material_id_df,on='material_type')
			# df_approved = df_approved.merge(carrier_id_df,on='vendor')
			df_approved['Invoice Date'] = pd.to_datetime(df_approved['Invoice Date']).dt.strftime('%d/%m/%Y')
			df_approved['Invoice Date']=df_approved['Invoice Date'].astype(str)
			df_approved[['drop_address_id','material_type_id']] = df_approved[['drop_address_id','material_type_id']].astype(int)

			print('after merge \n',df_approved)
			print("column names in rej : ",df_disapproved)
			df_disapproved = df_disapproved.drop_duplicates(subset = ['to_state','to_city','to_location'] ,keep = 'last')
			print("len of df_approved and df_disapproved",len(df_approved),len(df_disapproved))
			
			#df_approved = df_approved.merge(result2,on='vehicle_type')
			
			#df_approved_id = df_approved[['customer_id','branch_id','warehouse_id','rate_type']]
			#df_approved = df_approved.drop(['rate_type'],axis = 1)
			#df_approved_id = df_approved_id[:1]
			#df_approved = df_approved.dropna(axis = 0)
			
			#df_approved.rename(columns = {'rate':'agreed_price'}, inplace = true)
			#json_load_id=json.loads(df_approved_id.to_json(orient='records'))
			df_approved = df_approved.reset_index(drop=True)
			df_approved['id'] = df_approved.index
			json_load_final=json.loads(df_approved.to_json(orient='records'))
			json_output['drops']=json_load_final
			#json_output['ids']=json_load_id
			json_output['success']="true"
			json_output['message']='Success'

		else:
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
			json_output['success']="false"
			json_output['message']="unsuccessful"
		print("len(df_disapproved):",len(df_disapproved))
		if len(df_disapproved)>0 and len(df_approved) > 0:
			print("entering for df_approved and df_disapproved len > 0")
			json_output['success']="true"
			json_output['message']="partial"
			json_rejected_drops = json.loads(df_disapproved.to_json(orient='records'))
			json_output['rejectdata']=json_rejected_drops
	except:
		logging.info("An exception was thrown!", exc_info=True)
		return jsonify(json.loads('{"data":[], "success":"false", "message":"Invalid data." , "rejectdata":[],"ids":[] }'))
	finally:
		cur1.close()
		conn1.close()
		print('ok')
		
		
	return jsonify(json_output)

@app.route('/p_l_report', methods = ['POST'])

def p_l_report():
	try:
		content = request.get_json()
		region=content["region"]
		branch=content["branch"]
		customer=content["customer"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		
		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query1='''
		select * from 
	(select
		branch.branch_name as region,
		warehouse.warehouse_name as branch,
		trip_consignment.customer_lr_number,
		(case when customer_lr_numbers.used_datetime IS NULL then customer_lr_numbers.added_datetime else customer_lr_numbers.used_datetime end)::date as lr_date,
		customer.customer_company as customer,
		source.address_name as from_location,
		drops.address_name as to_location,
		booking_commercial.logistic_booking_type as service,
		trip_consignment.weight as actual_weight,
		shipment_details.charged_shipment_weight as carrier_charged_weight,
		booking_commercial.customer_sub_total,
		booking_commercial.carrier_sub_total,
		(booking_commercial.customer_sub_total - booking_commercial.carrier_sub_total) as p_and_l
	from booking_commercial
	join trip_consignment on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
	inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
	inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
	inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
	inner join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	join trip_track on trip_track.trip_id = trip.id
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join source on booking.source_id = source.id
	inner join drops on booking.final_drop_id = drops.id
	inner join branch on branch.id = booking.branch_id
	left join warehouse on cast(booking.warehouse_id as integer) = warehouse.id
	inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
	inner join customer on booking_commercial.customer_id = customer.id
	inner join company on vehicle.company_id = company.id

	inner join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id
		 inner join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id
	where booking_commercial.logistic_booking_type = 'LTL' and trip_track.trip_close = True
	UNION
	select
		branch.branch_name as region,
		warehouse.warehouse_name as branch,
		trip_consignment.customer_lr_number,
		(case when customer_lr_numbers.used_datetime IS NULL then customer_lr_numbers.added_datetime else customer_lr_numbers.used_datetime end)::date as lr_date,
		customer.customer_company as customer,
		source.address_name as from_location,
		drops.address_name as to_location,
		booking_commercial.logistic_booking_type as service,
		trip_consignment.weight as actual_weight,
		shipment_details.charged_shipment_weight as carrier_charged_weight,
		booking_commercial.customer_price as customer_sub_total,
		booking_commercial.carrier_price as carrier_sub_total,
		(booking_commercial.customer_price - booking_commercial.carrier_price) as p_and_l
	from booking_commercial
	join trip_consignment on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
	inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
	inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
	inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
	inner join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	join trip_track on trip_track.trip_id = trip.id
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join source on booking.source_id = source.id
	inner join drops on booking.final_drop_id = drops.id
	inner join branch on branch.id = booking.branch_id
	left join warehouse on cast(booking.warehouse_id as integer) = warehouse.id
	inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
	inner join customer on booking_commercial.customer_id = customer.id
	inner join company on vehicle.company_id = company.id
	inner join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id
	where booking_commercial.logistic_booking_type = 'FTL' and trip_track.trip_close = True
	)mytable
	where lr_date BETWEEN '{0}' AND '{1}'
		'''.format(from_date,to_date)
			#print(query)
			cur1.execute(query1)
			result1 = cur1.fetchall()
			
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			cur1.close()
			conn1.close()
		
		result=pd.DataFrame(result1,columns = ['Region', 'branch','customer_lr_number', 'lr_date', 'customer', 'from_location', 'to_location', 'service', 'actual_weight', 'carrier_charged_weight', 'customer_sub_total', 'carrier_sub_total', 'p_and_l'])
		print("checking length \n ",len(result))
		#print(result.customer)
			#newdf = result[(result.Region=="{0}") & (result.Branch=="{1}") & (result.customer_name=="{2}") & result.lr_date > '2020-11-10' & result.lr_date : '2020-11-15']
			#filtered_df=result.query(lr_date >= '2020-11-10' )
			#print(newdf)
		filters = result

		if region != 'All':
			filt = 'Region =="{0}" '.format(region)
			filters = filters.query(filt)
		
		if customer != 'All':
			filt1 = 'customer =="{0}" '.format(customer)
			filters = filters.query(filt1)
		
		if branch != 'All':
			filt2 = 'branch =="{0}" '.format(branch)
			filters = filters.query(filt2)

		#filt = 'Region =="{0}"  & customer == "{1}" & branch == "{2}" | Region =="{0}"  & customer == "{1}" '.format(region,customer,branch)
		#filters = result.query(filt)

		filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
		filters['lr_date']=filters['lr_date'].astype(str)
		print("entering")
		#result = list(filter)
		df = filters.to_json(orient="records")
		df = json.loads(df)
		s = {'cells':df,"success":"true"}
		
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"P&L report is currently unavailable."}')
		return jsonify(json_output)

@app.route('/pod_report', methods = ['POST'])
def pod_report():
	try:
		content = request.get_json()

		region=content["region"]
		branch=content["branch"]
		customer=content["customer"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query='''
		select * from
	(select 
		branch.branch_name as Region,
		warehouse.warehouse_name as Branch,
		customer.customer_company as customer, 
		trip_consignment.customer_lr_number as lr_number,
		shipment_details.time_stamp::date as lr_date,
		source.address_name as from_location,
		drops.address_name as to_location,
		booking.logistic_booking_type as service,
		to_char(t2.eta,'dd/mm/yyyy') as eta,
		t3.actual_delivery_date,
		(case when shipment_details.customer_pod_copy_link IS NOT NULL then shipment_details.customer_pod_copy_link else 'Not Uploaded' end ) as POD_status

	from trip_track 
	inner join trip on trip.id = trip_track.trip_id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
	inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
	inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
	

	inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
	inner join company on vehicle.company_id = company.id
	inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join source on booking.source_id = source.id
	inner join drops on booking.final_drop_id = drops.id
	inner join branch on branch.id = booking.branch_id
	left join warehouse on cast(booking.warehouse_id as integer) = warehouse.id 
	inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
	inner join customer on booking_commercial.customer_id = customer.id

	join
		(
			SELECT 
				booking_commercial.id as booking_commercial_id,
				(t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
			where  t_a.event_id = 4
		)as t2 
	on t2.booking_commercial_id = booking_commercial.id
	inner join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id
	where trip_track.trip_close = True
	)mytable
	where lr_date BETWEEN '{0}' AND '{1}';
		'''.format(from_date,to_date)
			#print(query)
			cur1.execute(query)
			result = cur1.fetchall()
			#print(result)
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			#raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			cur1.close()
			conn1.close()
			
		#result=pd.DataFrame(result)
		result=pd.DataFrame(result,columns = ["Region",'branch',"customer","lr_number","lr_date","from_location","to_location","service","eta","actual_delivery_date","POD_status"])
		print("checking length \n ",len(result))
		#print(result.customer)
			#newdf = result[(result.Region=="{0}") & (result.Branch=="{1}") & (result.customer_name=="{2}") & result.lr_date > '2020-11-10' & result.lr_date : '2020-11-15']
			#filtered_df=result.query(lr_date >= '2020-11-10' )
			#print(newdf)
		filters = result

		if region != 'All':
			filt = 'Region =="{0}" '.format(region)
			filters = filters.query(filt)
		
		if customer != 'All':
			filt1 = 'customer =="{0}" '.format(customer)
			filters = filters.query(filt1)
		
		if branch != 'All':
			filt2 = 'branch =="{0}" '.format(branch)
			filters = filters.query(filt2)
			
		#filt = 'Region =="{0}"  & customer == "{1}" & branch == "{2}" |Region =="{0}"  & customer == "{1}" '.format(region,customer,branch)
		#filters = result.query(filt)
		#print("checking 1 \n ",result[['Region','customer']])
		filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
		filters['actual_delivery_date'] = pd.to_datetime(filters['actual_delivery_date']).dt.strftime('%d/%m/%Y')
		filters['eta'] = pd.to_datetime(filters['eta']).dt.strftime('%d/%m/%Y')

		filters['lr_date']=filters['lr_date'].astype(str)
		filters['actual_delivery_date']=filters['actual_delivery_date'].astype(str)
		filters['eta']=filters['eta'].astype(str)
		print("entering \n",filters)
		#result = list(filter)
		df = filters.to_json(orient="records")
		df = json.loads(df)
		s = {'cells':df,"success":"true"}
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"POD report is currently unavailable."}')
		return jsonify(json_output)

@app.route('/annexure_report_invoicing', methods = ['POST'])
def annexure_report_invoicing():
	try:
		content = request.get_json()
		region=content["region"]
		branch=content["branch"]
		customer=content["customer"]
		service = content["service"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query='''
		select * from
	(select
	warehouse.warehouse_name as branch,
	booking_commercial.id as booking_commercial_id,	
	booking_commercial.logistic_booking_type as service,
	shipment_details.charged_shipment_weight as customer_charged_weight,
	booking_commercial.customer_price_per_kg,
	booking_commercial.customer_basic_freight,
	booking_commercial.customer_fsc_value,
	booking_commercial.customer_fov_value,
	booking_commercial.customer_docket_charge,
	booking_commercial.customer_oda_value as oda_charges,
	booking_commercial.customer_loading_charge,
	booking_commercial.customer_unloading_charge,
	booking_commercial.customer_other_charge,
	booking_commercial.customer_management_fee,
	(booking_commercial.customer_basic_freight + booking_commercial.customer_fsc_value + booking_commercial.customer_fov_value + booking_commercial.customer_docket_charge + shipment_details.handling_charges + booking_commercial.customer_oda_value  + booking_commercial.customer_loading_charge + booking_commercial.customer_unloading_charge + booking_commercial.customer_other_charge + booking_commercial.customer_management_fee) as sub,
	booking_commercial.customer_sgst,
	booking_commercial.customer_cgst,
	booking_commercial.customer_igst,
	(booking_commercial.customer_basic_freight + booking_commercial.customer_fsc_value + booking_commercial.customer_fov_value + booking_commercial.customer_docket_charge + shipment_details.handling_charges + booking_commercial.customer_oda_value + booking_commercial.customer_loading_charge + booking_commercial.customer_unloading_charge + booking_commercial.customer_other_charge + booking_commercial.customer_management_fee + booking_commercial.customer_sgst + booking_commercial.customer_cgst + booking_commercial.customer_igst) as total,
	branch.branch_name as Region,
	--	warehouse.warehouse_name as Branch,
	customer.customer_company,
	trip_consignment.customer_lr_number,
	trip_consignment.material_quantity as packages,
	trip_consignment.weight as actual_weight,
	shipment_details.time_stamp::date as lr_date,
	source.address_name as from_location,
	drops.address_name as to_location,
	shipment_details.handling_charges as detention_charges,
	shipment_details.invoice_no,
	(case when shipment_details.customer_pod_copy_link IS NOT NULL then shipment_details.customer_pod_copy_link else 'Not Uploaded' end ) as POD_status,
	shipment_details.invoice_value,
	t3.actual_delivery_date,
	one.invoice_date
	--trip_documents.added_on_datetime::date as invoice_date

	from trip_track 
	inner join trip on trip.id = trip_track.trip_id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
	inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
	inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
	inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
	inner join company on vehicle.company_id = company.id
	inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join source on booking.source_id = source.id
	inner join drops on booking.final_drop_id = drops.id
	inner join branch on branch.id = booking.branch_id
	left join warehouse on cast(booking.warehouse_id as integer) = warehouse.id 
	inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
	-- inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
	inner join customer on booking_commercial.customer_id = customer.id

	left join (select trip_id,added_on_datetime::date as invoice_date from trip_documents where trip_documents.document_type = 'invoice' group by trip_id,added_on_datetime)one on one.trip_id= trip_track.trip_id
	inner join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id
	where trip_track.trip_close = True

	)mytable
	where lr_date BETWEEN '{0}' AND '{1}';
		
		'''.format(from_date,to_date)
			#print(query)
			cur1.execute(query)
			result = cur1.fetchall()
			#print(result)
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			cur1.close()
			conn1.close()
			
		#result=pd.DataFrame(result)
		result=pd.DataFrame(result,columns = ['branch','booking_commercial_id', 'service', 'customer_charged_weight', 'customer_price_per_kg', 'customer_basic_freight', 'customer_fsc_value', 'customer_fov_value', 'customer_docket_charge', 'oda_charges', 'customer_loading_charge', 'customer_unloading_charge', 'customer_other_charge', 'customer_management_fee', 'sub', 'customer_sgst', 'customer_cgst', 'customer_igst', 'total', 'region', 'customer_name', 'customer_lr_number', 'packages', 'actual_weight', 'lr_date', 'from_location', 'to_location', 'detention_charges', 'invoice_no', 'pod_status', 'invoice_value', 'actual_delivery_date', 'invoice_date'])
		print("checking length \n ",len(result))
		#print(result.customer)
			#newdf = result[(result.Region=="{0}") & (result.Branch=="{1}") & (result.customer_name=="{2}") & result.lr_date > '2020-11-10' & result.lr_date : '2020-11-15']
			#filtered_df=result.query(lr_date >= '2020-11-10' )
			#print(newdf)
		if service == 'LTL':
			filters = result

			if region != 'All':
				filt = 'region =="{0}" '.format(region)
				filters = filters.query(filt)
			
			if customer != 'All':
				filt1 = 'customer_name =="{0}" '.format(customer)
				filters = filters.query(filt1)
			
			if service != 'All':
				filt2 = 'service =="{0}" '.format(service)
				filters = filters.query(filt2)
			#filt = 'region =="{0}" & customer_name == "{1}" & service == "{2}"'.format(region,customer,service)
		else:
			filters = result

			if region != 'All':
				filt = 'region =="{0}" '.format(region)
				filters = filters.query(filt)
			
			if customer != 'All':
				filt1 = 'customer_name =="{0}" '.format(customer)
				filters = filters.query(filt1)
			
			if service != 'All':
				filt2 = 'service =="{0}" '.format(service)
				filters = filters.query(filt2)
			
			if branch != 'All':
				filt2 = 'branch =="{0}" '.format(branch)
				filters = filters.query(filt2)
				
			#filt = 'region =="{0}" & customer_name == "{1}" & service == "{2}" & branch == "{3}"'.format(region,customer,service,branch)
		#filters = result.query(filt)

		filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
		filters['actual_delivery_date'] = pd.to_datetime(filters['actual_delivery_date']).dt.strftime('%d/%m/%Y')
		filters['invoice_date'] = pd.to_datetime(filters['invoice_date']).dt.strftime('%d/%m/%Y')

		filters['lr_date']=filters['lr_date'].astype(str)
		filters['actual_delivery_date']=filters['actual_delivery_date'].astype(str)
		filters['invoice_date']=filters['invoice_date'].astype(str)
		filters['sub'].fillna(0,inplace = True)
		filters['total'].fillna(0,inplace = True)
		filters['customer_sgst'].fillna(0,inplace = True)
		filters['customer_igst'].fillna(0,inplace = True)
		filters['customer_cgst'].fillna(0,inplace = True)

		sub = sum(filters['sub'])
		customer_sgst = sum(filters['customer_sgst'])
		customer_igst = sum(filters['customer_igst'])
		customer_cgst = sum(filters['customer_cgst'])
		total = sum(filters['total'])
		#print(filters[['lr_date','customer_name']])	
		print("entering")
		#result = list(filter)
		df = filters.to_json(orient="records")

		df = json.loads(df)
		s = {'cells':df,'c_val':[{'sub':sub,'customer_sgst':customer_sgst,'customer_igst':customer_igst,'customer_cgst':customer_cgst,'total':total}],"success":"true"}
		
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"Annexure for invoice report is currently unavailable."}')
		return jsonify(json_output)
		
@app.route('/annexure_report_vendor', methods = ['POST'])
def annexure_report_vendor():
	try:
		content = request.get_json()
		region=content["region"]
		branch=content["branch"]
		vendor=content["vendor"]
		service = content["service"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query='''
		select * from
	(
	select
		
		booking_commercial.id as booking_commercial_id,
		branch.branch_name as region,
		warehouse.warehouse_name as Branch,
		trip_consignment.customer_lr_number,
		shipment_details.time_stamp::date as lr_date,
		-- customer_lr_numbers.customer_lr_number,
		--(case when customer_lr_numbers.used_datetime IS NULL then customer_lr_numbers.added_datetime else customer_lr_numbers.used_datetime end)::date as lr_date,
		company.company_name as vendor,
		shipment_details.vendor_lr_number,
		shipment_details.vendor_lr_date as vendor_lr_date,
		source.address_name as from_location,
		drops.address_name as to_location,
		booking_commercial.logistic_booking_type as service,
		trip_consignment.material_quantity as packages,
		trip_consignment.weight as actual_weight,
		booking_commercial.carrier_charged_weight,
		booking_commercial.carrier_price_per_kg,
		booking_commercial.carrier_basic_freight,
		booking_commercial.carrier_fsc_value,
		booking_commercial.carrier_fov_value,
		booking_commercial.carrier_docket_charge,
		shipment_details.handling_charges as detention_charges,
		booking_commercial.carrier_oda_value as oda_charges,
		booking_commercial.carrier_loading_charge,
		booking_commercial.carrier_unloading_charge,
		booking_commercial.carrier_other_charge,
		(booking_commercial.carrier_basic_freight + booking_commercial.carrier_fsc_value + booking_commercial.carrier_fov_value + booking_commercial.carrier_docket_charge + shipment_details.handling_charges + booking_commercial.carrier_oda_value  + booking_commercial.carrier_loading_charge + booking_commercial.carrier_unloading_charge + booking_commercial.carrier_other_charge ) as sub,
		booking_commercial.carrier_sgst,
		booking_commercial.carrier_cgst,
		booking_commercial.carrier_igst,
		(booking_commercial.carrier_igst + booking_commercial.carrier_cgst + booking_commercial.carrier_sgst + booking_commercial.carrier_basic_freight + booking_commercial.carrier_fsc_value + booking_commercial.carrier_fov_value + booking_commercial.carrier_docket_charge + shipment_details.handling_charges + booking_commercial.carrier_oda_value  + booking_commercial.carrier_loading_charge + booking_commercial.carrier_unloading_charge + booking_commercial.carrier_other_charge )as total_expense,
		shipment_details.invoice_no,
		shipment_details.invoice_value,
		one.invoice_date,
		(booking_commercial.carrier_igst + booking_commercial.carrier_cgst + booking_commercial.carrier_sgst+shipment_details.invoice_value::numeric) as total_amount
	 
	from trip_track 
	inner join trip on trip.id = trip_track.trip_id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
	inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
	inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
	

	inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
	inner join company on vehicle.company_id = company.id
	inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join source on booking.source_id = source.id
	inner join drops on booking.final_drop_id = drops.id
	inner join branch on branch.id = booking.branch_id
	left join warehouse on cast(booking.warehouse_id as integer) = warehouse.id
	inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
	-- inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
	inner join customer on booking_commercial.customer_id = customer.id

	left join (select trip_id,added_on_datetime::date as invoice_date from trip_documents where trip_documents.document_type = 'invoice' group by trip_id,added_on_datetime)one on one.trip_id= trip_track.trip_id

	inner join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id

	where trip_track.trip_close = True
	)mytable
	where lr_date BETWEEN '{0}' AND '{1}'
	ORDER BY lr_date ;
		
		'''.format(from_date,to_date)
			#print(query)
			cur1.execute(query)
			result = cur1.fetchall()
			#print(result)
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			cur1.close()
			conn1.close()
			
		#result=pd.DataFrame(result)
		result=pd.DataFrame(result,columns = ['booking_commercial_id', 'region','branch', 'customer_lr_number', 'lr_date', 'vendor', 'vendor_lr_number', 'vendor_lr_date', 'from_location', 'to_location', 'service', 'packages', 'actual_weight', 'carrier_charged_weight', 'carrier_price_per_kg', 'carrier_basic_freight', 'carrier_fsc_value', 'carrier_fov_value', 'carrier_docket_charge', 'detention_charges', 'oda_charges', 'carrier_loading_charge', 'carrier_unloading_charge', 'carrier_other_charge', 'sub', 'carrier_sgst', 'carrier_cgst', 'carrier_igst', 'total_expense', 'invoice_no', 'invoice_value', 'invoice_date', 'total_amount'])
		print("checking length \n ",len(result))
		#print(result.customer)
			#newdf = result[(result.Region=="{0}") & (result.Branch=="{1}") & (result.customer_name=="{2}") & result.lr_date > '2020-11-10' & result.lr_date : '2020-11-15']
			#filtered_df=result.query(lr_date >= '2020-11-10' )
			#print(newdf)
		if service == 'LTL':
			filters = result

			if region != 'All':
				filt = 'region =="{0}" '.format(region)
				filters = filters.query(filt)
			
			if vendor != 'All':
				filt1 = 'vendor =="{0}" '.format(vendor)
				filters = filters.query(filt1)
			
			if service != 'All':
				filt2 = 'service =="{0}" '.format(service)
				filters = filters.query(filt2)
				
			filt = 'region =="{0}" & vendor == "{1}" & service == "{2}"'.format(region,vendor,service)
		else:
			filters = result

			if region != 'All':
				filt = 'region =="{0}" '.format(region)
				filters = filters.query(filt)
			
			if vendor != 'All':
				filt1 = 'vendor =="{0}" '.format(vendor)
				filters = filters.query(filt1)
			
			if service != 'All':
				filt2 = 'service =="{0}" '.format(service)
				filters = filters.query(filt2)
				
			if branch != 'All':
				filt2 = 'branch =="{0}" '.format(branch)
				filters = filters.query(filt2)
			
			#filt = 'region =="{0}" & vendor == "{1}" & service == "{2}" & branch == "{3}"'.format(region,vendor,service,branch)
		#filters = result.query(filt)

		filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
		filters['invoice_date'] = pd.to_datetime(filters['invoice_date']).dt.strftime('%d/%m/%Y')

		filters['lr_date']=filters['lr_date'].astype(str)
		filters['invoice_date']=filters['invoice_date'].astype(str)
		
		filters['sub'].fillna(0,inplace = True)
		filters['total_expense'].fillna(0,inplace = True)
		filters['carrier_sgst'].fillna(0,inplace = True)
		filters['carrier_igst'].fillna(0,inplace = True)
		filters['carrier_cgst'].fillna(0,inplace = True)
		
		sub = sum(filters['sub'])
		carrier_sgst = sum(filters['carrier_sgst'])
		carrier_igst = sum(filters['carrier_igst'])
		carrier_cgst = sum(filters['carrier_cgst'])
		total_expense = sum(filters['total_expense'])

		print("entering")
		#result = list(filter)
		df = filters.to_json(orient="records")
		print(df)
		df = json.loads(df)
		s = {'cells':df,'c_val':[{'sub':sub,'carrier_sgst':carrier_sgst,'carrier_igst':carrier_igst,'carrier_cgst':carrier_cgst,'total_expense':total_expense}],"success":"true"}
		
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"Annexure for vendor report is currently unavailable."}')
		return jsonify(json_output)

@app.route('/thc_report', methods = ['POST'])
def thc_report():
	try:
		content = request.get_json()
		region=content["region"]
		branch=content["branch"]
		vendor=content["vendor"]
		service = content["service"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		company_id = content["company_id"]

		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query='''select * from 
	(
		select
		booking_commercial.logistic_booking_type as service,
		thc_details.branch as region,
		warehouse.warehouse_name as branch,
		thc_details.lr_no,
		thc_details.lr_date::date as lr_date,
		thc_details.thc_number,
		thc_details.vendor as vendor_name,
		thc_details.vendor_code,
		thc_details.thc_creation_time as thc_date,
		thc_details.vehicle_number,
		thc_details.vehicle_type,
		thc_payment_charges.loading_charges,
		thc_payment_charges.final_amount as thc_cost,
		
	--	(case when thc_payments.payment_type = 'Advance' then thc_payments.time_stamp else Null end)as advance_date, 
		--adv.advance_date,
		thc_payment_charges.halting_charges,
		thc_payment_charges.unloading_charges,
		thc_payment_charges.Police_RTO,
		thc_payment_charges.misc_charges,
		(thc_payment_charges.loading_charges+thc_payment_charges.advance_amount+thc_payment_charges.halting_charges+thc_payment_charges.unloading_charges+thc_payment_charges.Police_RTO+thc_payment_charges.misc_charges) as sub_total,
		thc_payment_charges.tds,
		thc_payment_charges.advance_check_no as advance_amount_check_no,
		thc_payment_charges.advance_check_date as advance_amount_check_date,
		-- done --thc_payment_charges.time_stamp::date as advance_amount_check_date,
		thc_payment_charges.advance_amount,
		 (case when thc_payment_charges.advance_check_no is NOT NULL  then thc_payment_charges.advance_amount else Null end) advance_check_amount,
		thc_payment_charges.balance,
		thc_payment_charges.final_check_no as final_amount_check_no,
		--fadv.final_payment_date,
		thc_payment_charges.final_check_date as final_payment_date,
		-- done--(case when thc_payments.payment_type = 'Final' then thc_payment_charges.final_amount else Null end) as amount,
		(case when thc_payments.payment_status = 'Completed' then thc_payment_charges.balance else NULL end)as amount,
		(case when thc_payments.payment_status = 'Completed' then 'THC closed' else 'THC open' end)as remarks
		from thc_masters 
	 join thc_details on thc_masters.thc_masters_id = thc_details.thc_masters_id
	 join thc_payments on thc_payments.thc_masters_id = thc_masters.thc_masters_id
	 join thc_payment_charges on thc_payment_charges.thc_masters_id = thc_masters.thc_masters_id
	inner join branch on branch.id = thc_masters.branch_id
	 join trip_consignment on thc_details.drop_id = trip_consignment.drop_id
	 join trip_track on trip_track.master_trip_id = thc_masters.master_trip_id
	 inner join trip on trip.id = trip_track.trip_id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join users on users.id = booking.user_id	
	left JOIN COMPANY ON company.id = users.company_id
	 join booking_commercial on trip_consignment.trip_consignment_id = booking_commercial.trip_consignment_id
	 join warehouse on cast(booking.warehouse_id as integer) = warehouse.id
	where company.id = '{2}'
	)mytable
		WHERE	 
			lr_date BETWEEN '{0}' AND '{1}'
		ORDER BY lr_date;
		
		'''.format(from_date,to_date,company_id)
			##print(query)
			cur1.execute(query)
			result = cur1.fetchall()
			print(result)
			result=pd.DataFrame(result,columns = ['service', 'region','branch', 'lr_no', 'lr_date', 'thc_number', 'vendor_name', 'vendor_code', 'thc_date', 'vehicle_number', 'vehicle_type', 'loading_charges', 'thc_cost', 'halting_charges', 'unloading_charges', 'police_rto', 'misc_charges', 'sub_total', 'tds', 'advance_amount_check_no', 'advance_amount_check_date', 'advance_amount', 'advance_check_amount', 'balance', 'final_amount_check_no', 'final_payment_date', 'amount', 'remarks'])
			print("checking length \n ",len(result))
			#print(result.customer)
				#newdf = result[(result.Region=="{0}") & (result.Branch=="{1}") & (result.vendor_name=="{2}") & result.lr_date > '2020-11-10' & result.lr_date : '2020-11-15']
				#filtered_df=result.query(lr_date >= '2020-11-10' )
				#print(newdf)
			filters = result

			if region != 'All':
				filt = 'region =="{0}" '.format(region)
				filters = filters.query(filt)
			
			if vendor != 'All':
				filt1 = 'vendor_name =="{0}" '.format(vendor)
				filters = filters.query(filt1)
			
			if service != 'All':
				filt2 = 'service =="{0}" '.format(service)
				filters = filters.query(filt2)
				
			if branch != 'All':
				filt2 = 'branch =="{0}" '.format(branch)
				filters = filters.query(filt2)
				
			#filt = 'region =="{0}" & vendor_name == "{1}" & service == "{2}" & branch == "{3}"'.format(region,vendor,service,branch)
			#filters = result.query(filt)
			print(filters)
			filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
			filters['thc_date'] = pd.to_datetime(filters['thc_date']).dt.strftime('%d/%m/%Y')
			#filters['advance_date'] = pd.to_datetime(filters['advance_date']).dt.strftime('%d/%m/%Y')
			filters['advance_amount_check_date'] = pd.to_datetime(filters['advance_amount_check_date']).dt.strftime('%d/%m/%Y')
			filters['final_payment_date'] = pd.to_datetime(filters['final_payment_date']).dt.strftime('%d/%m/%Y')
			
			filters['lr_date']=filters['lr_date'].astype(str)
			filters['thc_date']=filters['thc_date'].astype(str)
			#filters['advance_date']=filters['advance_date'].astype(str)	
			filters['advance_amount_check_date']=filters['advance_amount_check_date'].astype(str)
			filters['final_payment_date']=filters['final_payment_date'].astype(str)
			
			print("entering")
			#result = list(filter)
			df = filters.to_json(orient="records")
			df = json.loads(df)
			s = {'cells':df,"success":"true"}
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			cur1.close()
			conn1.close()
			
		
		
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"THC report is currently unavailable."}')
		return jsonify(json_output)
		
@app.route('/lr_customer_report', methods = ['POST'])
def lr_customer_report():
	content = request.get_json()
	customer=content["customer"]

	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     select booking.logistic_booking_type,
customer.customer_company,
count(trip_consignment.customer_lr_number) as lr_count
from booking join trip_consignment 
	on cast(booking.final_drop_id as integer)=trip_consignment.drop_id 
inner join customer
	on customer.id = booking.customer_id::INTEGER inner join trip_track on booking.final_drop_id = trip_track.drop_id where customer.customer_company='{0}' and trip_close = True group by (booking.logistic_booking_type,customer.customer_company)
	 )mytable
	
	'''.format(customer)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result)
		print("checking length \n ",len(result))
		result = list(result[0])
		print(result)
		s = {'cells':result}
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		s = {'cells':[]}
		conn1.rollback()
		logging.error("Database connection error")
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
	
	return jsonify(s)

@app.route('/lr_region_report', methods = ['POST'])
def lr_region_report():
	content = request.get_json()

	#customer=content["customer"]
	region=content["region"]
	# vendor=content["vendor"]
	# from_date=content["from_date"]
	# to_date=content["to_date"]
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     select booking.logistic_booking_type,
branch.branch_name,
count(trip_consignment.customer_lr_number) as lr_count
from booking join trip_consignment 
	on cast(booking.final_drop_id as integer)=trip_consignment.drop_id 
inner join branch
	on branch.id = booking.branch_id inner join trip_track on booking.final_drop_id = trip_track.drop_id where branch.branch_name = '{0}' and trip_close = True group by (booking.logistic_booking_type,branch.branch_name)
	 )mytable
	
	'''.format(region)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result)
		print(result)
		
		result = list(result[0])
		print(result)
		s = {'cells':result}
		
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		conn1.rollback()
		logging.error("Database connection error")
		s = {'cells':[]}
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
		
	
	
	return jsonify(s)

@app.route('/lr_branch_report', methods = ['POST'])
def lr_branch_report():
	content = request.get_json()

	#customer=content["customer"]
	branch=content["branch"]
	# vendor=content["vendor"]
	# from_date=content["from_date"]
	# to_date=content["to_date"]
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     select booking.logistic_booking_type,
warehouse.warehouse_name as branch,
count(trip_consignment.customer_lr_number) as lr_count 
from booking join trip_consignment 
	on cast(booking.final_drop_id as integer)=trip_consignment.drop_id 
inner join branch
	on branch.id = booking.branch_id
join trip_track on booking.final_drop_id = trip_track.drop_id
left join warehouse on warehouse.id = cast(booking.warehouse_id as integer) 	where warehouse.warehouse_name = '{0}' and trip_track.trip_close = True group by (booking.logistic_booking_type,warehouse.warehouse_name)
	)mytable
	
	'''.format(branch)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result = list(result[0])
		print(result)
		s = {'cells':result}
		
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		conn1.rollback()
		logging.error("Database connection error")
		s = {'cells':[]}
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()

	
	return jsonify(s)

@app.route('/pod_customer_report', methods = ['POST'])
def pod_customer_report():
	content = request.get_json()

	customer=content["customer"]
	# branch=content["branch"]
	# vendor=content["vendor"]
	# from_date=content["from_date"]
	# to_date=content["to_date"]
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     select booking.logistic_booking_type,
customer.customer_company,
count(shipment_details.customer_pod_copy_link) as pod_count
from booking join shipment_details 
	on cast(booking.final_drop_id as integer)=shipment_details.drop_id 
inner join customer
	on customer.id = shipment_details.customer_id inner join trip_track on booking.final_drop_id = trip_track.drop_id where customer.customer_company='{0}' and trip_close = True group by (booking.logistic_booking_type,customer.customer_company)
	 )mytable
	
	'''.format(customer)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result = list(result[0])
		print(result)
		s = {'cells':result}
		
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		conn1.rollback()
		logging.error("Database connection error")
		s = {'cells':[]}
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
		

	
	return jsonify(s)

@app.route('/pod_region_report', methods = ['POST'])
def pod_region_report():
	content = request.get_json()

	#customer=content["customer"]
	region=content["region"]
	# vendor=content["vendor"]
	# from_date=content["from_date"]
	# to_date=content["to_date"]
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     select booking.logistic_booking_type,
branch.branch_name,
count(shipment_details.customer_pod_copy_link) as pod_count
from booking join shipment_details 
	on cast(booking.final_drop_id as integer)=shipment_details.drop_id 
inner join branch
	on branch.id = booking.branch_id inner join trip_track on booking.final_drop_id = trip_track.drop_id where branch.branch_name='{0}' and trip_track.trip_close = True group by (booking.logistic_booking_type,branch.branch_name)
	 )mytable
	
	'''.format(region)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result = list(result[0])
		print(result)
		s = {'cells':result}
		
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		conn1.rollback()
		logging.error("Database connection error")
		s = {'cells':[]}
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
	
	return jsonify(s)


@app.route('/pod_branch_report', methods = ['POST'])
def pod_branch_report():
	content = request.get_json()

	#customer=content["customer"]
	branch=content["branch"]
	# vendor=content["vendor"]
	# from_date=content["from_date"]
	# to_date=content["to_date"]
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     select booking.logistic_booking_type,
warehouse.warehouse_name,
count(shipment_details.customer_pod_copy_link) as pod_count 
from booking join shipment_details 
	on cast(booking.final_drop_id as integer)=shipment_details.drop_id 
inner join branch
	on branch.id = booking.branch_id 
join trip_track on booking.final_drop_id = trip_track.drop_id
left join warehouse on warehouse.id = cast(booking.warehouse_id as integer)  where warehouse.warehouse_name='{0}' and trip_track.trip_close = True group by (booking.logistic_booking_type,warehouse.warehouse_name)
	 )mytable
	
	'''.format(branch)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result = list(result[0])
		print(result)
		s = {'cells':result}
		
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		conn1.rollback()
		logging.error("Database connection error")
		
		s = {'cells':[]}
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
		
	
	return jsonify(s)

@app.route('/lr_report', methods = ['POST'])
def lr_report():
	try:
		content = request.get_json()

		region=content["region"]
		branch=content["branch"]
		customer=content["customer"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query='''
	select * from 
	(
		select
		t2.eta as expected,
		trip_consignment.road_distance as tot_kms,
		t3.actual_delivery_date::date as to_date,
		t3.actual_delivery_date::time as actual_delivery_time,
		EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta)) as delivery_tat,
		 trip_track.master_trip_id as trip_id,

		vehicle_booking_details.vehicle_id as vehicle_id,

		t7.start_date,

		branch.branch_name as region,
		warehouse.warehouse_name as branch,
		customer.customer_company,

	trip_consignment.customer_lr_number,
	trip_consignment.material_quantity as packages,
	shipment_details.time_stamp::date as lr_date,
		  drops.name as consigneename,

		   source.name as consignorname,

			source.address_name as from_location,

			drops.address_name as to_location,
		booking_commercial.logistic_booking_type as service,
		trip_consignment.weight as actual_weight,
		shipment_details.charged_shipment_weight as customer_charged_weight,

		shipment_details.invoice_no,
		shipment_details.invoice_value,
		shipment_details.ewaybillno,
		one.invoice_date,
	   thc_details.vehicle_number as vehicle_no,
	 thc_details.vehicle_type as model_of_truck,

		thc_details.driver_mobile_no as driver_number,
		 booking_commercial.customer_tat as tat,
			t1.unloading_time,
			t1.unloading_date,	
			'' as halting_charges

	from trip_track 
	inner join trip on trip.id = trip_track.trip_id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
	inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
	inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id

	inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
	inner join company on vehicle.company_id = company.id
	inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join branch on branch.id = booking.branch_id
	left join warehouse on warehouse.id = cast(booking.warehouse_id as integer)
	inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
	inner join customer on booking_commercial.customer_id = customer.id
	left join thc_masters on thc_masters.master_trip_id = trip_track.master_trip_id
	left join thc_details on thc_masters.thc_masters_id = thc_details.thc_masters_id
	left join (select trip_id,added_on_datetime::date as invoice_date from trip_documents where trip_documents.document_type = 'invoice' group by trip_id,added_on_datetime)one on one.trip_id= trip_track.trip_id
	inner join source on booking.source_id = source.id
	inner join drops on trip_track.drop_id = drops.id
	inner join
		(
			SELECT 
				tr.id as trip_id,
				ROUND((EXTRACT(EPOCH FROM(t_b.event_time - t_a.event_time))/3600)::numeric,3) AS unloading_time,
				t_b.event_time::date as unloading_date
			FROM 
				trip_events t_a 
			CROSS JOIN trip_events t_b
			inner join trip tr on 
				tr.id = t_a.trip_id
			where t_b.event_id = 11 and t_a.event_id = 10 and t_a.trip_id = t_b.trip_id and t_a.trip_id = tr.id
		)as t1
	on trip.id = t1.trip_id 
	join
		(
			SELECT 
				booking_commercial.id as booking_commercial_id,
				(t_a.time_stamp::timestamp::date) AS eta
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
			where  t_a.event_id = 4
		)as t2 
	on t2.booking_commercial_id = booking_commercial.id

	inner join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id

	inner join 
		(SELECT trip_events.event_time::date as start_date,trip.id
			FROM 
				trip_events  
			join trip on trip.id = trip_events.trip_id
			where trip_events.event_id = 4
		)as t7
	on t7.id = trip.id
	where trip_track.trip_close = True
	)mytable

	WHERE lr_date BETWEEN '{0}' AND '{1}'

		
		'''.format(from_date,to_date)
			#print(query)
			cur1.execute(query)
			result = cur1.fetchall()
			result=pd.DataFrame(result,columns = ['expected','tot_kms','to_date','actual_delivery_time','delivery_tat','trip_id', 'vehicle_id', 'start_date', 'region', 'branch','customer', 'customer_lr_number','packages', 'lr_date', 'consigneename', 'consignorname', 'from_location', 'to_location', 'service', 'actual_weight','customer_charged_weight','invoice_no',  'invoice_value', 'ewaybillno','invoice_date',  'vehicle_no', 'model_of_truck', 'driver_name', 'tat', 'unloading_time', 'unloading_date', 'halting_charges'])
			print("checking length \n ",len(result))
			filters = result
			if region != 'All':
				filt = 'region =="{0}" '.format(region)
				filters = filters.query(filt)
			
			if customer != 'All':
				filt1 = 'customer =="{0}" '.format(customer)
				filters = filters.query(filt1)

			if branch != 'All':
				filt2 = 'branch =="{0}" '.format(branch)
				filters = filters.query(filt2)
			

			#filt = 'region =="{0}"  & customer == "{1}" & branch == "{2}" | region =="{0}"  & customer == "{1}" '.format(region,customer,branch)
			#filters = result.query(filt)
			print("checking \n ",filters)
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			cur1.close()
			conn1.close()
		filters['tat'] = filters['tat'].fillna(0)
		max_columns = int(max(filters['delivery_tat'],default=0) + 1)
		#filters['start_date'] = pd.to_datetime(filters['start_date'], format='%Y-%m-%d')
		ctr = 0
		for i in range(1,max_columns): # this loop creates columns based on tat and dates are generated 
			name = 'day {0}'.format(i)
			ctr = ctr + 1 #ctr is day count
			filters.insert(ctr, name, "")
		#iteration through eacch row:
		for row in filters.index:
			tat_val = int(filters.loc[row,'delivery_tat'] + 1)
			
			start = filters.loc[row,'start_date']
			dates = []
			dates.append(start)
			for i in range(2,tat_val):
				start = start + datetime.timedelta(days=1)
				dates.append(start) #make it empty in the end
				#print(dates)
			#need to have vehicle_id
			set_location_to_col = 1
			for all_date in dates:
				day_column_num = 'day {0}'.format(set_location_to_col)
				trip_no = filters.loc[row,'trip_id']
				vehicle_id = filters.loc[row,'vehicle_id']
				#print(day_column_num,trip_no,vehicle_id)
				conn1 = psycopg2.connect(dbname="ezyloads", user="ezyloads", host="127.0.0.1", password="ezy@1234")
				cur1 = conn1.cursor()
				query = """ select lat_log from (select lattitude ||','|| longitude as lat_log,location_time::date as dat from waypoints_v_part_{0} where master_trip_id = {1} order by location_time::date desc)mytable where dat = '{2}' ; """.format(vehicle_id,trip_no,all_date)
				cur1.execute(query)
				wayp_res = cur1.fetchone()
				locations = "{0}".format(wayp_res)
				locations = locations[2:23]
				print(locations)
				#latlog = '12.982811,77.6385579' 
				try:
					#print('getting address')
					address = getplace(locations)
					
				except:
					print('not getting address')
					address = ''
				#pprint(address)
				#print(locations)
				cur1.close()
				conn1.close()
				filters.loc[row,day_column_num] = address
				set_location_to_col = set_location_to_col + 1
				if set_location_to_col > filters.loc[row,'delivery_tat']:
					#print("entering")
					set_location_to_col = 0 
			dates = []
		
		filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
		filters['invoice_date'] = pd.to_datetime(filters['invoice_date']).dt.strftime('%d/%m/%Y')
		filters['unloading_date'] = pd.to_datetime(filters['unloading_date']).dt.strftime('%d/%m/%Y')
		filters['to_date'] = pd.to_datetime(filters['to_date']).dt.strftime('%d/%m/%Y')
		filters['start_date'] = pd.to_datetime(filters['start_date']).dt.strftime('%d/%m/%Y')
		filters['expected'] = pd.to_datetime(filters['expected']).dt.strftime('%d/%m/%Y')
		
		filters['lr_date']=filters['lr_date'].astype(str)
		filters['to_date']=filters['to_date'].astype(str)
		filters['start_date']=filters['start_date'].astype(str)
		filters['invoice_date']=filters['invoice_date'].astype(str)
		filters['unloading_date']=filters['unloading_date'].astype(str)
		filters['expected']=filters['expected'].astype(str)

		print("entering \n",filters)
		#result = list(filter)
		df = filters.to_json(orient="records")
		df = json.loads(df)
		s = {'cells':df,'master_tat':max(filters['delivery_tat'],default = 0),"success":"true"}
		
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"LR report is currently unavailable."}')
		return jsonify(json_output)
@app.route('/editable_annexure_invoice', methods = ['POST'])
def editable_annexure_invoice():
	try:
		content = request.get_json()
		val = list(content.keys())
		bk_commercialid=content["bk_commercialid"]
		print(type(val))
		oda_charges=content[val[1]]
		#customer_loading_charge=content["customer_loading_charge"]
		#customer_unloading_charge=content["customer_unloading_charge"]
		#customer_other_charge=content["customer_other_charge"]
		#print(oda_charges,bk_commercialid)
		if oda_charges is not None:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query = '''UPDATE booking_commercial SET {0} = {1} WHERE id = {2} '''.format(val[1],oda_charges,bk_commercialid)
			cur1.execute(query)
			conn1.commit()
			cur1.close()
			conn1.close()
			json_output = json.loads('{"success":"True","message":"Annexure Invoice edited successfully." }')
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"Failure","message":"Annexure Invoice was unsuccessful." }')
	return jsonify(json_output)

@app.route('/editable_annexure_vendor', methods = ['POST'])
def editable_annexure_vendor():
	try:
		content = request.get_json()
		bk_commercialid=content["bk_commercialid"]
		carrier_other_charge=content["carrier_other_charge"]
		#print(oda_charges,bk_commercialid)
		if carrier_other_charge is not None:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query = '''UPDATE booking_commercial SET carrier_other_charge = {0} WHERE id = {1} '''.format(carrier_other_charge,bk_commercialid)
			cur1.execute(query)
			conn1.commit()
			cur1.close()
			conn1.close()
		json_output = json.loads('{"success":"True","message":"Annexure Vendor edited successfully." }')
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"Failure","message":"Annexure Vendor was unsuccessful." }')
	return jsonify(json_output)
	
@app.route('/kpi_report', methods = ['POST'])
def kpi_report():
	try:
		content = request.get_json()
		region=content["region"]
		branch=content["branch"]
		customer=content["customer"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query='''
	-- KPI Report
	select * from 
		(select 
			branch.branch_name as Region,
			warehouse.warehouse_name as Branch,

		   customer.customer_company as customer,
			trip.id as trip_id,
			trip_consignment.customer_lr_number,
			trip_consignment.material_quantity as packages,
			trip_consignment.weight as actual_weight,


			shipment_details.time_stamp::date as lr_date,
			
			t2.eta::date as expected_date_delivery,
			
			source.address_name as from_location,

			drops.address_name as to_location,

			t3.actual_delivery_date::date,

		shipment_details.charged_shipment_weight as charged_weight,
		booking_commercial.customer_tat as approx_transit_days,
		booking_commercial.logistic_booking_type as service,
		 (case when (booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))) )::INTEGER < 0 OR (booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))) )::INTEGER > 1 then 100 else ((booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))))*100)::INTEGER end) as percentage,

			 

			 shipment_details.invoice_no as invoice_number,
			 shipment_details.invoice_value as invoice_value,
			one.invoice_date,
			--(case when trip_documents.added_on_datetime::date is not null then trip_documents.added_on_datetime::date else null end)as invoice_date,

			company.company_name as vendor_name,

			 vehicle.regno as vehicle_no,

			 vehicle_attr.model as model_of_truck,

			 EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta)) as delivery_tat
		
			
			
		from trip_track 
	inner join trip on trip.id = trip_track.trip_id
	inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
	inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
	inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
	

	inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
	inner join company on vehicle.company_id = company.id
	inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
	inner join booking on booking.id = vehicle_booking_details.booking_id
	inner join source on booking.source_id = source.id
	inner join drops on booking.final_drop_id = drops.id
	inner join branch on branch.id = booking.branch_id
	left join warehouse on warehouse.id = cast(booking.warehouse_id as integer)
	inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
	-- inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
	inner join customer on booking_commercial.customer_id = customer.id
	left join thc_details on trip_consignment.drop_id = thc_details.drop_id
	left join thc_masters on thc_details.thc_masters_id = thc_masters.thc_masters_id
	left join thc_payment_charges on thc_payment_charges.thc_masters_id = thc_masters.thc_masters_id

	left join (select trip_id,added_on_datetime::date as invoice_date from trip_documents where trip_documents.document_type = 'invoice' group by trip_id,added_on_datetime)one on one.trip_id= trip_track.trip_id

	join
		(
			SELECT 
				booking_commercial.id as booking_commercial_id,
				(t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
			where  t_a.event_id = 4
		)as t2 
	on t2.booking_commercial_id = booking_commercial.id

	inner join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 inner join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id
	where trip_track.trip_close = True
	--  
	)mytable
	WHERE
		lr_date BETWEEN '{0}' AND '{1}'
		'''.format(from_date,to_date)
			print(query)
			cur1.execute(query)
			result = cur1.fetchall()
			result=pd.DataFrame(result,columns = ['region','branch','customer', 'trip_id', 'customer_lr_number', 'packages', 'actual_weight', 'lr_date', 'expected_date_delivery', 'from_location', 'to_location', 'actual_delivery_date', 'charged_weight', 'approx_transit_days', 'service', 'percentage', 'invoice_number', 'invoice_value', 'invoice_date', 'vendor_name', 'vehicle_no', 'model_of_truck', 'delivery_tat'])
			print("checking length \n ",len(result))
			filters = result
			if region != 'All':
				filt = 'region =="{0}" '.format(region)
				filters = filters.query(filt)
			
			if customer != 'All':
				filt1 = 'customer =="{0}" '.format(customer)
				filters = filters.query(filt1)

			if branch != 'All':
				filt2 = 'branch =="{0}" '.format(branch)
				filters = filters.query(filt2)
				
			#filt = 'region =="{0}"  & customer == "{1}" & branch == "{2}" | region =="{0}"  & customer == "{1}" '.format(region,customer,branch)
			#filters = result.query(filt)

			filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
			filters['actual_delivery_date'] = pd.to_datetime(filters['actual_delivery_date']).dt.strftime('%d/%m/%Y')
			filters['expected_date_delivery'] = pd.to_datetime(filters['expected_date_delivery']).dt.strftime('%d/%m/%Y')
			filters['invoice_date'] = pd.to_datetime(filters['invoice_date']).dt.strftime('%d/%m/%Y')

			filters['lr_date'] = filters['lr_date'].astype(str)
			filters['actual_delivery_date'] = filters['actual_delivery_date'].astype(str)
			filters['expected_date_delivery']=filters['expected_date_delivery'].astype(str)	
			filters['invoice_date']=filters['invoice_date'].astype(str)
			print("entering")
			df = filters.to_json(orient="records")
			df = json.loads(df)
			s = {'cells':df,"success":"true"}
			
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			cur1.close()
			conn1.close()
		
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"KPI report is currently unavailable."}')
		return jsonify(json_output)
		
@app.route('/dsr_report', methods = ['POST'])
def dsr_report():
	try:
		content = request.get_json()
		region=content["region"]
		branch=content["branch"]
		customer=content["customer"]
		from_date=content["from_date"]
		to_date=content["to_date"]
		company_id = content["company_id"]

		try:
			conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
			cur1 = conn1.cursor()
			query='''
	select * from 
	(select 
	warehouse.warehouse_name as branch,
	branch.branch_name as Region,
	trip_consignment.customer_lr_number,
	trip_consignment.material_quantity as packages,
	shipment_details.time_stamp::date as lr_date,
	t2.eta as expected_date_delivery,
	customer.customer_company as customer,
	source.address_name as from_location,
	drops.address_name as to_location,
	shipment_details.invoice_no,
	shipment_details.vendor_lr_number,
	shipment_details.vendor_lr_date as vendor_lr_date,
	one.invoice_date,
	-- trip_documents.added_on_datetime::date as invoice_date,
	thc_details.vendor as vendor_name,
	thc_details.thc_number,
	thc_details.thc_creation_date as thc_date,
	thc_details.vehicle_number,
	thc_details.vehicle_type,
	trip_consignment.weight as actual_weight,
	booking_commercial.carrier_volumetric_weight as volumetric_weight,
	shipment_details.charged_shipment_weight as charged_weight,
	booking_commercial.logistic_booking_type as service,
	booking_commercial.customer_basic_freight,
	booking_commercial.customer_fsc,
	booking_commercial.customer_fov,
	booking_commercial.customer_docket_charge,
	booking_commercial.customer_handing_charge,
	booking_commercial.customer_oda,
	booking_commercial.customer_loading_charge,
	booking_commercial.customer_unloading_charge,
	booking_commercial.customer_other_charge,
	booking_commercial.customer_management_fee,
	(COALESCE(booking_commercial.customer_management_fee,0)+COALESCE(booking_commercial.customer_other_charge,0)+COALESCE(booking_commercial.customer_unloading_charge,0)+COALESCE(booking_commercial.customer_loading_charge,0)+COALESCE(booking_commercial.customer_oda,0)+COALESCE(booking_commercial.customer_handing_charge,0)+COALESCE(booking_commercial.customer_docket_charge,0)+COALESCE(booking_commercial.customer_basic_freight,0) + COALESCE(booking_commercial.customer_fsc,0) + COALESCE(booking_commercial.customer_fov,0)) as sub_total,
	(COALESCE(booking_commercial.customer_sgst,0) + COALESCE(booking_commercial.customer_cgst,0) + COALESCE(booking_commercial.customer_igst,0)) as gst,
	(COALESCE(booking_commercial.customer_management_fee,0)+COALESCE(booking_commercial.customer_other_charge,0)+COALESCE(booking_commercial.customer_unloading_charge,0)+COALESCE(booking_commercial.customer_loading_charge,0)+COALESCE(booking_commercial.customer_oda,0)+COALESCE(booking_commercial.customer_handing_charge,0)+COALESCE(booking_commercial.customer_docket_charge,0)+COALESCE(booking_commercial.customer_basic_freight,0) + COALESCE(booking_commercial.customer_fsc,0) + COALESCE(booking_commercial.customer_fov,0) + COALESCE(booking_commercial.customer_sgst,0) + COALESCE(booking_commercial.customer_cgst,0) + COALESCE(booking_commercial.customer_igst,0))as customer_total_freight,
	booking_commercial.carrier_fsc,
	booking_commercial.carrier_fov,
	booking_commercial.carrier_docket_charge,
	booking_commercial.carrier_oda,
	booking_commercial.carrier_loading_charge,
	booking_commercial.carrier_unloading_charge,
	booking_commercial.carrier_other_charge,
	(booking_commercial.carrier_sgst + booking_commercial.carrier_cgst + booking_commercial.carrier_igst) as carrier_gst,
	booking_commercial.carrier_basic_freight as vendor_basic_cost,
	thc_payment_charges.final_amount,
	thc_payment_charges.advance_amount,
	thc_payment_charges.balance,
	thc_payment_charges.halting_charges,
	(COALESCE(thc_payment_charges.halting_charges,0)+COALESCE(booking_commercial.carrier_other_charge,0)+COALESCE(booking_commercial.carrier_unloading_charge,0)+COALESCE(booking_commercial.carrier_loading_charge,0)+COALESCE(booking_commercial.carrier_oda,0)+COALESCE(booking_commercial.carrier_docket_charge,0)+COALESCE(booking_commercial.carrier_fov,0)+COALESCE(booking_commercial.carrier_fsc,0)+COALESCE(thc_payment_charges.balance,0)+COALESCE(thc_payment_charges.advance_amount,0)+COALESCE(thc_payment_charges.final_amount,0))as sub_total,
	(COALESCE(thc_payment_charges.halting_charges,0)+COALESCE(booking_commercial.carrier_sgst,0) + COALESCE(booking_commercial.carrier_cgst,0) + COALESCE(booking_commercial.carrier_igst,0) + COALESCE(booking_commercial.carrier_other_charge,0)+COALESCE(booking_commercial.carrier_unloading_charge,0)+COALESCE(booking_commercial.carrier_loading_charge,0)+COALESCE(booking_commercial.carrier_oda,0)+COALESCE(booking_commercial.carrier_docket_charge,0)+COALESCE(booking_commercial.carrier_fov,0)+COALESCE(booking_commercial.carrier_fsc,0)+COALESCE(thc_payment_charges.balance,0)+COALESCE(thc_payment_charges.advance_amount,0)+COALESCE(thc_payment_charges.final_amount,0)) as total_vendor_expense,
	(COALESCE(booking_commercial.customer_management_fee,0)+COALESCE(booking_commercial.customer_other_charge,0)+COALESCE(booking_commercial.customer_unloading_charge,0)+COALESCE(booking_commercial.customer_loading_charge,0)+COALESCE(booking_commercial.customer_oda,0)+COALESCE(booking_commercial.customer_handing_charge,0)+COALESCE(booking_commercial.customer_docket_charge,0)+COALESCE(booking_commercial.customer_basic_freight,0) + COALESCE(booking_commercial.customer_fsc,0) + COALESCE(booking_commercial.customer_fov,0))-(COALESCE(thc_payment_charges.halting_charges,0)+COALESCE(booking_commercial.carrier_other_charge,0)+COALESCE(booking_commercial.carrier_unloading_charge,0)+COALESCE(booking_commercial.carrier_loading_charge,0)+COALESCE(booking_commercial.carrier_oda,0)+COALESCE(booking_commercial.carrier_docket_charge,0)+COALESCE(booking_commercial.carrier_fov,0)+COALESCE(booking_commercial.carrier_fsc,0)+COALESCE(thc_payment_charges.balance,0)+COALESCE(thc_payment_charges.advance_amount,0)+COALESCE(thc_payment_charges.final_amount,0)) as docket_profitability,
	t3.actual_delivery_date
	from
    trip_track
    join trip on trip.id = trip_track.trip_id
    join shipment_details on shipment_details.drop_id = trip_track.drop_id
    join drops on drops.id = trip_track.drop_id
	
    join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
    join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
    left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
    join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
    join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
    join vehicle_type on vehicle_attr. vehicle_type_id = vehicle_type.id
    join booking on booking.id = vehicle_booking_details.booking_id
	 inner join branch on branch.id = booking.branch_id
	left join warehouse on cast(booking.warehouse_id as integer) = warehouse.id
    left join source on booking.source_id = source.id
	LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	inner join users on users.id = booking.user_id	
	left JOIN COMPANY ON company.id = users.company_id
	 inner join customer on booking_commercial.customer_id = customer.id
	left join gps_device_provider on gps_device_provider.id = vehicle.gps_device_provider
	 left join thc_masters on thc_masters.master_trip_id = trip_track.master_trip_id
	left join thc_details on thc_masters.thc_masters_id = thc_details.thc_masters_id
	left join thc_payments on thc_payments.thc_masters_id = thc_masters.thc_masters_id
	left join thc_payment_charges on thc_payment_charges.thc_masters_id = thc_masters.thc_masters_id
	left join (select trip_id,added_on_datetime::date as invoice_date from trip_documents where trip_documents.document_type = 'invoice' group by trip_id,added_on_datetime)one on one.trip_id= trip_track.trip_id

   
join
		(
			SELECT 
				booking_commercial.id as booking_commercial_id,
				(t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
			from trip_consignment
		 left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 left join trip_events t_a on tr.id = t_a.trip_id
			where  t_a.event_id = 4
		)as t2 
	on t2.booking_commercial_id = booking_commercial.id
		 
	left join 
		(SELECT booking_commercial.id as booking_commercial_id,
			t_a.event_time AS actual_delivery_date
			from trip_consignment
		 left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
		 left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		 left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
		 left join trip_events t_a on tr.id = t_a.trip_id
		where t_a.event_id = 12
		)as t3
	on t3.booking_commercial_id = booking_commercial.id
	LEFT JOIN (SELECT EVENT_TIME, TRIP_ID
					FROM TRIP_EVENTS
		   			WHERE EVENT_ID = 4) AS TRIP_EVENT_START ON TRIP.ID = TRIP_EVENT_START.TRIP_ID
left JOIN (SELECT EVENT_TIME, TRIP_ID
				FROM TRIP_EVENTS
		   			WHERE EVENT_ID = 12) AS TRIP_EVENT_END ON TRIP.ID = TRIP_EVENT_END.TRIP_ID
	where trip_track.trip_close = True and company.id = '{2}'

	)mytable
		WHERE	 
		lr_date BETWEEN '{0}' AND '{1}'
		'''.format(from_date,to_date,company_id)
			print(from_date)
			cur1.execute(query)
			result = cur1.fetchall()
		except Exception:
			logging.info("An exception was thrown!", exc_info=True)
			conn1.rollback()
			logging.error("Database connection error")
			raise
		#for i in result:
		else:
			conn1.commit()
		finally:
			#cur1.close()
			conn1.close()
			
		result=pd.DataFrame(result,columns=['branch','region','customer_lr_number','packages','lr_date','expected_date_delivery','customer','from_location','to_location','invoice_no','vendor_lr_number','vendor_lr_date','invoice_date','vendor_name','thc_number','thc_date','vehicle_number','vehicle_type','actual_weight','volumetric_weight','charged_weight','service','customer_basic_freight','customer_fsc','customer_fov','customer_docket_charge','customer_handing_charge','customer_oda','customer_loading_charge','customer_unloading_charge','customer_other_charge','customer_management_fee','cust_sub_total','gst','customer_total_freight','carrier_fsc','carrier_fov','carrier_docket_charge','carrier_oda','carrier_loading_charge','carrier_unloading_charge','carrier_other_charge','carrier_gst','vendor_basic_cost','final_amount','advance_amount','balance','halting_charges','sub_total','total_vendor_expense','docket_profitability','actual_delivery_date'])
		print("checking length \n ",len(result))
		filters = result
		if region != 'All':
			filt = 'region =="{0}" '.format(region)
			filters = filters.query(filt)
			
		if customer != 'All':
			filt1 = 'customer =="{0}" '.format(customer)
			filters = filters.query(filt1)

		if branch != 'All':
			filt2 = 'branch =="{0}" '.format(branch)
			filters = filters.query(filt2)
			
		#filt = 'region =="{0}"  & customer == "{1}" & branch == "{2}" | region =="{0}" & customer == "{1}" '.format(region,customer,branch)
		#filters = result.query(filt)
		print("after executing filter query:",len(filters))
		filters['lr_date'] = pd.to_datetime(filters['lr_date']).dt.strftime('%d/%m/%Y')
		filters['actual_delivery_date'] = pd.to_datetime(filters['actual_delivery_date']).dt.strftime('%d/%m/%Y')
		filters['expected_date_delivery'] = pd.to_datetime(filters['expected_date_delivery']).dt.strftime('%d/%m/%Y')
		filters['invoice_date'] = pd.to_datetime(filters['invoice_date']).dt.strftime('%d/%m/%Y')
		filters['vendor_lr_date'] = pd.to_datetime(filters['vendor_lr_date']).dt.strftime('%d/%m/%Y')
		filters['thc_date'] = pd.to_datetime(filters['thc_date']).dt.strftime('%d/%m/%Y')

		filters['lr_date']=filters['lr_date'].astype(str)
		filters['expected_date_delivery']=filters['expected_date_delivery'].astype(str)	
		filters['actual_delivery_date']=filters['actual_delivery_date'].astype(str)
		filters['invoice_date']=filters['invoice_date'].astype(str)
		filters['vendor_lr_date']=filters['vendor_lr_date'].astype(str)
		filters['thc_date']=filters['thc_date'].astype(str)
		
		print("entering",len(filters))
		#result = list(filter)
		df = filters.to_json(orient="records")
		df = json.loads(df)
		s = {'cells':df,"success":"true"}
		#if (len(df) == 0):
		#	s = {'cells':df,"success":"true","message":"No Data for the selected filter."}
		#result = list(result[0])
		
		
		return jsonify(s)
	except:
		logging.info("An exception was thrown!", exc_info=True)
		json_output = json.loads('{"success":"false","message":"Annexure for vendor report is currently unavailable."}')
		return jsonify(json_output)
		
@app.route('/pl_customer_report', methods = ['POST'])
def pl_customer_report():
	content = request.get_json()

	customer=content["customer"]
	# branch=content["branch"]
	# vendor=content["vendor"]
	# from_date=content["from_date"]
	# to_date=content["to_date"]
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     SELECT
customer.customer_company as customer,
booking_commercial.logistic_booking_type as service,
sum(booking_commercial.customer_sub_total - booking_commercial.carrier_sub_total) as p_and_l
from trip_track 
inner join trip on trip.id = trip_track.trip_id
inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
inner join customer on booking_commercial.customer_id = customer.id
 where customer.customer_company like'{0}%' and booking_commercial.logistic_booking_type = 'LTL' and trip_track.trip_close = True group by customer.customer_company,booking_commercial.logistic_booking_type
 UNION
SELECT
customer.customer_company as customer,
booking_commercial.logistic_booking_type as service,
sum(booking_commercial.customer_price - booking_commercial.carrier_price) as p_and_l
from trip_track 
inner join trip on trip.id = trip_track.trip_id
inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
inner join customer on booking_commercial.customer_id = customer.id
 where customer.customer_company like'{0}%' and trip_track.trip_close = True and booking_commercial.logistic_booking_type = 'FTL' group by customer.customer_company,booking_commercial.logistic_booking_type 
	 )mytable
	
	'''.format(customer)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result)
		print(result)
		result = list(result[0])
		print(result)
		s = {'cells':result}
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		s = {'cells':[]}
		conn1.rollback()
		logging.error("Database connection error")
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
	
	return jsonify(s)

@app.route('/pl_region_report', methods = ['POST'])
def pl_region_report():
	content = request.get_json()

	#customer=content["customer"]
	region=content["region"]
	# vendor=content["vendor"]
	# from_date=content["from_date"]
	# to_date=content["to_date"]
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     SELECT
branch.branch_name,
booking_commercial.logistic_booking_type as service,
sum(booking_commercial.customer_sub_total - booking_commercial.carrier_sub_total) as p_and_l
from booking_commercial
join trip_consignment on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
inner join trip on trip.vehicle_booking_details = vehicle_booking_details.id
inner join booking on booking.id = vehicle_booking_details.booking_id
inner join branch on branch.id = booking.branch_id
inner join trip_track on booking.final_drop_id = trip_track.drop_id
where branch.branch_name like '{0}%' and booking_commercial.logistic_booking_type = 'LTL' and trip_track.trip_close = True
group by booking_commercial.logistic_booking_type,branch.branch_name
UNION
SELECT
branch.branch_name,
booking_commercial.logistic_booking_type as service,
sum(booking_commercial.customer_price - booking_commercial.carrier_price) as p_and_l
from booking_commercial
join trip_consignment on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
inner join trip on trip.vehicle_booking_details = vehicle_booking_details.id
inner join booking on booking.id = vehicle_booking_details.booking_id
inner join branch on branch.id = booking.branch_id
inner join trip_track on booking.final_drop_id = trip_track.drop_id
where branch.branch_name like '{0}%' and booking_commercial.logistic_booking_type = 'FTL'  and trip_track.trip_close = True
group by booking_commercial.logistic_booking_type,branch.branch_name)mytable
	
	'''.format(region)
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result)
		print(result)
		
		result = list(result[0])
		print(result)
		s = {'cells':result}
		
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		conn1.rollback()
		logging.error("Database connection error")
		s = {'cells':[]}
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
		
	
	
	return jsonify(s)
	
@app.route('/pl_wise_report', methods = ['POST'])
def pl_wise_report():
	content = request.get_json()
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
     SELECT 
booking_commercial.logistic_booking_type as service,
sum(booking_commercial.customer_sub_total - booking_commercial.carrier_sub_total) as p_and_l
from booking_commercial
join trip_consignment on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
inner join booking on booking.id = vehicle_booking_details.booking_id
join trip_track on booking.final_drop_id = trip_track.drop_id
where booking_commercial.logistic_booking_type = 'LTL' and trip_track.trip_close = True
group by booking_commercial.logistic_booking_type
UNION
SELECT 
booking_commercial.logistic_booking_type as service,
sum(booking_commercial.customer_price - booking_commercial.carrier_price) as p_and_l
from booking_commercial
join trip_consignment on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
inner join booking on booking.id = vehicle_booking_details.booking_id
join trip_track on booking.final_drop_id = trip_track.drop_id
 where booking_commercial.logistic_booking_type = 'FTL' and trip_track.trip_close = True
group by booking_commercial.logistic_booking_type)mytable
	'''
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result)
		print(result)
		result = list(result[0])
		print(result)
		s = {'cells':result}
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		s = {'cells':[]}
		conn1.rollback()
		logging.error("Database connection error")
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
	
	return jsonify(s)

@app.route('/pl_branch_report', methods = ['POST'])
def pl_branch_report():
	content = request.get_json()
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''select row_to_json(mytable) from 
    (
SELECT
warehouse.warehouse_name,
booking_commercial.logistic_booking_type as service,
sum(booking_commercial.customer_price - booking_commercial.carrier_price) as p_and_l
from booking_commercial
join trip_consignment on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
inner join customer_lr_numbers on trip_consignment.customer_lr_numbers_id = customer_lr_numbers.id
inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
inner join trip on trip.vehicle_booking_details = vehicle_booking_details.id
inner join booking on booking.id = vehicle_booking_details.booking_id
left join warehouse on warehouse.id = booking.warehouse_id::integer
join trip_track on booking.final_drop_id = trip_track.drop_id
where warehouse.warehouse_name like '{0}%' and booking_commercial.logistic_booking_type = 'FTL' and trip_track.trip_close = True
group by booking_commercial.logistic_booking_type,warehouse.warehouse_name
)mytable
	'''
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result)
		print(result)
		result = list(result[0])
		print(result)
		s = {'cells':result}
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		s = {'cells':[]}
		conn1.rollback()
		logging.error("Database connection error")
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
	
	return jsonify(s)


@app.route('/kpi_on_time', methods = ['POST'])
def kpi_on_time():
	content = request.get_json()
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''
     select customer,avg(percentage) from 
    (select 
        branch.branch_name as Region,
    --  warehouse.warehouse_name as Branch,

       customer.customer_company as customer,
        trip_consignment.customer_lr_number,
        shipment_details.time_stamp::date as lr_date,
        t2.eta::date as expected_date_delivery,

 	    t3.actual_delivery_date::date,
     (case when (booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))) )::INTEGER < 0 OR (booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))) )::INTEGER > 1 then 100 else ((booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))))*100)::INTEGER end) as percentage
   
    from trip_track 
inner join trip on trip.id = trip_track.trip_id
inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id

inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
inner join company on vehicle.company_id = company.id
inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
inner join booking on booking.id = vehicle_booking_details.booking_id
inner join branch on branch.id = booking.branch_id
inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
inner join customer on booking_commercial.customer_id = customer.id
left join thc_details on trip_consignment.drop_id = thc_details.drop_id
left join thc_masters on thc_details.thc_masters_id = thc_masters.thc_masters_id
left join thc_payment_charges on thc_payment_charges.thc_masters_id = thc_masters.thc_masters_id
join
    (
        SELECT 
            booking_commercial.id as booking_commercial_id,
            (t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
        from trip_consignment
     inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
     inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
     inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
     inner join trip_events t_a on tr.id = t_a.trip_id
        where  t_a.event_id = 4
    )as t2 
on t2.booking_commercial_id = booking_commercial.id

inner join 
    (SELECT booking_commercial.id as booking_commercial_id,
        t_a.event_time AS actual_delivery_date
        from trip_consignment
     inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
     inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
     inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
     inner join trip_events t_a on tr.id = t_a.trip_id
    where t_a.event_id = 12
    )as t3
on t3.booking_commercial_id = booking_commercial.id
where trip_track.trip_close = True
)mytable
group by customer

	
	'''
		print(query)
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result,columns = ['customer','percentage'])
		df = result.to_json(orient="records")
		df = json.loads(df)
		s = {'cells':df}
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		s = {'cells':[]}
		conn1.rollback()
		logging.error("Database connection error")
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()
	
	return jsonify(s)

@app.route('/kpi_delay_time', methods = ['POST'])
def kpi_delay_time():
	content = request.get_json()
	try:
		conn1 = psycopg2.connect(dbname="ezyloads",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()
		query='''
     select customer,(100 - avg(percentage)) as delay_percent from 
    (select 
        branch.branch_name as Region,
    --  warehouse.warehouse_name as Branch,

       customer.customer_company as customer,
        trip_consignment.customer_lr_number,
        shipment_details.time_stamp::date as lr_date,
        t2.eta::date as expected_date_delivery,

 	    t3.actual_delivery_date::date,
     (case when (booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))) )::INTEGER < 0 OR (booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))) )::INTEGER > 1 then 100 else ((booking_commercial.customer_tat - (EXTRACT(DAY FROM (t3.actual_delivery_date - t2.eta))))*100)::INTEGER end) as percentage
   
    from trip_track 
inner join trip on trip.id = trip_track.trip_id
inner join vehicle_booking_details on vehicle_booking_details.id = trip.vehicle_booking_details
inner join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id

inner join vehicle on vehicle_booking_details.vehicle_id = vehicle.id		
inner join company on vehicle.company_id = company.id
inner join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
inner join booking on booking.id = vehicle_booking_details.booking_id
inner join branch on branch.id = booking.branch_id
inner join shipment_details on booking.final_drop_id = shipment_details.drop_id
inner join customer on booking_commercial.customer_id = customer.id
left join thc_details on trip_consignment.drop_id = thc_details.drop_id
left join thc_masters on thc_details.thc_masters_id = thc_masters.thc_masters_id
left join thc_payment_charges on thc_payment_charges.thc_masters_id = thc_masters.thc_masters_id
join
    (
        SELECT 
            booking_commercial.id as booking_commercial_id,
            (t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
        from trip_consignment
     inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
     inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
     inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
     inner join trip_events t_a on tr.id = t_a.trip_id
        where  t_a.event_id = 4
    )as t2 
on t2.booking_commercial_id = booking_commercial.id

inner join 
    (SELECT booking_commercial.id as booking_commercial_id,
        t_a.event_time AS actual_delivery_date
        from trip_consignment
     inner join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
     inner join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
     inner join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
     inner join trip_events t_a on tr.id = t_a.trip_id
    where t_a.event_id = 12
    )as t3
on t3.booking_commercial_id = booking_commercial.id
where trip_track.trip_close = True
)mytable
group by customer
	
	'''
		cur1.execute(query)
		result = cur1.fetchall()
		result=pd.DataFrame(result,columns = ['customer','delay_percent'])
		df = result.to_json(orient="records")
		df = json.loads(df)
		s = {'cells':df}
	except Exception:
		logging.info("An exception was thrown!", exc_info=True)
		s = {'cells':[]}
		conn1.rollback()
		logging.error("Database connection error")
	#for i in result:
	else:
		conn1.commit()
	finally:
		cur1.close()
		conn1.close()

	return jsonify(s)


if __name__ == "__main__":
	app.run(host='demo2.transo.in', port=5000,ssl_context=('/etc/letsencrypt/live/demo2.transo.in/fullchain.pem', '/etc/letsencrypt/live/demo2.transo.in/privkey.pem'))
