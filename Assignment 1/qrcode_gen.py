import qrcode
# example data
data = "https://www.pinterest.com/pin/27373510214883341/"
# output file name
filename = "bookmakr.png"
# generate qr code
img = qrcode.make(data)
# save img to a file
img.save(filename)