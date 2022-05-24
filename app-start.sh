#Create venv if not present
if [ -d ./venv ]
then
    echo "virtual env already exists"
else
    echo "virtual env doesn't exist, creating it .."
    python3 -m venv ../venv
fi 

#Activate venv
echo "Activating venv"
. ./venv/bin/activate
python3 -m pip install -r requirements.txt