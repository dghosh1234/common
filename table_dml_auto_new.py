Option Explicit

Dim tbl
Dim tableList
tableList = "Tables in the current model:" & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        tableList = tableList & "- " & tbl.Name & vbCrLf
    End If
Next

MsgBox tableList, vbInformation, "DEBUG: Table List"


