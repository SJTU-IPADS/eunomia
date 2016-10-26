import re
import sys

def get_th(inpat, number):
	get_re = re.compile("GET Abort Counts (\d+) Succ Counts (\d+) Capacity Counts (\d+) Conflict Counts (\d+) Zero Counts (\d+)")
	end_re = re.compile("END Abort Counts (\d+) Succ Counts (\d+) Capacity Counts (\d+) Conflict Counts (\d+) Zero Counts (\d+)")
	krt_re = re.compile("KRTM (\d+), (\d+), (\d+)")
	rt_re = re.compile("RTM (\d+), (\d+), (\d+)")
	gabs = 0 ; gsuc = 0; gcap = 0; gcon = 0; gzro = 0; eabs = 0 ; esuc = 0; ecap = 0; econ = 0; ezro = 0
	kcon = 0; kcap = 0; koth = 0; ocon = 0; ocap = 0; ooth = 0
	
	for i in range(1, number+1):
		infile = inpat+str(i)
		indata = enumerate(open(infile))
	
		for (lno, line) in indata:
			if get_re.search(line):
				temp = get_re.search(line)
				gabs = gabs + int(temp.group(1))
				gsuc = gsuc + int(temp.group(2))
				gcap = gcap + int(temp.group(3))
				gcon = gcon + int(temp.group(4))
				gzro = gzro + int(temp.group(5))
			elif end_re.search(line):
				temp = end_re.search(line)
				eabs = eabs + int(temp.group(1))
				esuc = esuc + int(temp.group(2))
				ecap = ecap + int(temp.group(3))
				econ = econ + int(temp.group(4))
				ezro = ezro + int(temp.group(5))
			elif krt_re.search(line):
				temp = krt_re.search(line)
				kcon = kcon + int(temp.group(1))
				kcap = kcap + int(temp.group(2))
				koth = koth + int(temp.group(3))
			elif rt_re.search(line):
				temp = rt_re.search(line)
				ocon = ocon + int(temp.group(1))
				ocap = ocap + int(temp.group(2))
				ooth = ooth + int(temp.group(3))
			
	#print "GET "+str(gsuc/number) +"  "+str(gabs/number)+"  "+str(gcap/number)+"  "+str(gcon/number) + " "+str(gzro/number)
	print "END, "+str(esuc/number) +",  "+str(eabs/number)+" , "+str(ecap/number)+" , "+str(econ/number) + " ,"+str(ezro/number)
	print "KRTM, "+str(kcon/number)+","+str(kcap/number)+","+str(koth/number)
	print "RTM,"+str(ocon/number)+","+str(ocap/number)+","+str(ooth/number)
			

if __name__ == "__main__":
	get_th(sys.argv[1], int(sys.argv[2]))
