# change the name
class qbeOutput:
  def __init__(self, start_time):
    self.start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    self.end_time = '2021-05-14 10:31:00'
    self.file = []
    self.output_file= []
    self.msg = []
    # change the following field to fulfilled the requirement
    self.currmb=0
    self.precon=0
    self.acbmb=0
    self.actmb_actn= []
    self.actmb_currto= []
    self.pec= []
  def addfile(self,file_name,count):
    file_dict = {'file_name': file_name,'count':count}
    self.file.append(file_dict)
  def add_msg(self,msg):
    self.msg.append(msg)
class qbeOutputToTemplate:
  def __init__(self, qbe_output):
    self.start_time = qbe_output.start_time
    self.end_time= qbe_output.end_time
    self.file = self.get_file(qbe_output.file)
    self.output_file= self.get_output_file(qbe_output.output_file)
    self.msg= self.get_msg(qbe_output.msg)
    # change the following field to fulfilled the requirement
    self.currmb = qbe_output.currmb
    self.precon = qbe_output.precon
    self.acbmb = qbe_output.acbmb
    self.actmb_actn = self.get_actmb_actn(qbe_output.actmb_actn)
    self.actmb_currto = self.get_actmb_currto(qbe_output.actmb_currto)
    self.pec = self.get_pec(qbe_output.pec)

  def get_file(self,file):
    result = ''
    for val in file:
        result += val['file_name']+'(' + str(val['count']) + ')\n'
    return result

  def get_output_file(self, output_file):
    result = ''
    for val in output_file:
        result +=  val +'\n'
    return result

  def get_msg(self, msg):
    result = ''
    for val in msg:
        result +=  val +'\n'
    return result
# delete the following field to fulfilled the requirement
  def get_pec(self, pec):
    result = ''
    for val in pec:
        result +=  val +'\n'
    return result

  def get_actmb_actn(self,actmb_actn):
    result = ''
    for val in actmb_actn:
        result += val['ACTN']+'(' + str(val['COUNT']) + ')\n'
    return result
  
  def get_actmb_currto(self,actmb_currto):
    result = ''
    for val in actmb_currto:
        result += val['CURRTO'].strftime('%Y%m%d')+'(' + str(val['COUNT']) + ')\n'
    return result