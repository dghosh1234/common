Option Explicit

Dim tbl, col
Dim found, affectedCount
Dim report
Dim runCheck
Dim fileName, filePath
Dim file

' Initialize
affectedCount = 0
found = False
report = ""

' Specify the output file path
filePath = "C:\path\to\your\directory\" ' Adjust this path as needed
fileName = "Varchar2_Columns_Needing_Update_Report.txt"

' Open file for writing
Set file = CreateObject("Scripting.FileSystemObject").CreateTextFile(filePath & fileName, True)

' Write header to file
file.WriteLine("Report: Tables and Columns Needing Update to VARCHAR2(n CHAR)")
file.WriteLine("Generated on: " & Now)
file.WriteLine(String(80, "="))

' Scan all tables and columns
For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                ' Check for LengthSemantics property existence
                On Error Resume Next
                Dim colSemantics
                colSemantics = col.LengthSemantics
                If Err.Number <> 0 Then
                    ' If no LengthSemantics found, assume it's in BYTE, and needs update
                    If Trim(UCase(col.DataType)) = "VARCHAR2" Then
                        report = "Table: " & tbl.Code & " | Column: " & col.Code & " (Length: " & col.Length & ") needs update to VARCHAR2(n CHAR)"
                        file.WriteLine(report)
                        affectedCount = affectedCount + 1
                    End If
                    Err.Clear
                Else
                    ' If LengthSemantics is not CHAR, it's also an affected column
                    If Trim(UCase(col.LengthSemantics)) <> "CHAR" Then
                        report = "Table: " & tbl.Code & " | Column: " & col.Code & " (Length: " & col.Length & ") needs update to VARCHAR2(n CHAR)"
                        file.WriteLine(report)
                        affectedCount = affectedCount + 1
                    End If
                End If
                On Error GoTo 0
            End If
        Next
    End If
Next

' Write footer to file
file.WriteLine(String(80, "="))
file.WriteLine("Total affected columns: " & affectedCount)

' Close the file
file.Close()

' Display result message
If affectedCount = 0 Then
    MsgBox "No VARCHAR2 columns need updating in any table.", vbInformation, "No Updates Required"
Else
    MsgBox "Report generated: " & filePath & fileName & vbCrLf & "Total affected columns: " & affectedCount, vbInformation, "Report Generated"
End If
