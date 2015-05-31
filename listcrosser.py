l1 = []
l1_input = raw_input("List 1: ")
l1 = map(str, l1_input.split())

l2 = []
l2_input = raw_input("List 2: ")
l2 = map(str, l2_input.split())

for item1 in l1:
    for item2 in l2:
        print "%s:%s" % (item1, item2)
