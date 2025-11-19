from django import forms

class UploadCSVForm(forms.Form):
    arquivo_csv = forms.FileField(label='Selecione o arquivo CSV')